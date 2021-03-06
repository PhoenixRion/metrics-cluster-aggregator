/**
 * Copyright 2014 Groupon.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.tsdcore.scripting.lua;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.scripting.Alert;
import com.arpnetworking.tsdcore.scripting.ScriptingException;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.joda.time.Period;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Defines and supports evaluation of an alert (aka condition) using Lua. The
 * alert is defined for a fully qualified statistic name (FQSN) the value of
 * which is compared to a constant using the specified operator. If the result
 * of the comparison is <code>true</code> the alert is triggered.
 *
 * More complex alerts should first be transformed into statements as
 * EXPRESSION OPERATOR VALUE. Next, if the EXPRESSION is more complex than a
 * single FQSN it should be defined as an <code>Expression</code> instance.
 * This computes the value of the expression as a separate statistic and yields
 * a single FQSN appropriate for alerting.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class LuaAlert implements Alert {

    @Override
    public Condition evaluate(final PeriodicData periodicData) throws ScriptingException {
        // The alert may not apply to this period
        if (!periodicData.getPeriod().equals(_period)) {
            return new Condition.Builder()
                    .setName(_name)
                    .setFQDSN(_fqdsn)
                    .setThreshold(_value)
                    .setExtensions(_extensions)
                    .build();
        }

        // Retrieve the current value by FQDSN
        final Optional<AggregatedData> datum = periodicData.getDatumByFqdsn(_fqdsn);
        if (!datum.isPresent()) {
            return new Condition.Builder()
                    .setName(_name)
                    .setFQDSN(_fqdsn)
                    .setThreshold(_value)
                    .setExtensions(_extensions)
                    .build();
        }

        // Ensure both or neither Quantity has a unit
        if (_value.getUnit().isPresent() != datum.get().getValue().getUnit().isPresent()) {
            throw new ScriptingException(String.format(
                    "Cannot evaluate data with unit against value without unit; datum=%s, value=%s",
                    datum,
                    _value));
        }

        // Evaluate the alert condition
        final LuaValue result;
        try {
            final double datumValue;
            if (_value.getUnit().isPresent()) {
                datumValue = datum.get().getValue().convertTo(_value.getUnit().get()).getValue();
            } else {
                datumValue = datum.get().getValue().getValue();
            }

            final LuaValue value = LuaValue.valueOf(datumValue);
            result = _expression.call(value);
            // CHECKSTYLE.OFF: IllegalCatch - Lua throws RuntimeExceptions
        } catch (final Exception e) {
            // CHECKSTYLE.ON: IllegalCatch
            throw new ScriptingException(String.format("Expression evaluation failed; expression=%s", this), e);
        }

        return new Condition.Builder()
                .setName(_name)
                .setFQDSN(_fqdsn)
                .setThreshold(_value)
                .setTriggered(convertToBoolean(result).orElse(null))
                .setExtensions(_extensions)
                .build();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.<String, Object>builder()
                .put("name", _name)
                .put("fqdsn", _fqdsn)
                .put("period", _period)
                .put("operator", _operator)
                .put("value", _value)
                .put("extensions", _extensions)
                .build();
    }


    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private Optional<Boolean> convertToBoolean(final LuaValue result) throws ScriptingException {
        if (result.isboolean()) {
            return Optional.of(result.toboolean());
        }
        throw new ScriptingException(
                String.format("Script returned an unsupported value; result=%s", result));
    }

    private LuaAlert(final Builder builder) {
        _name = builder._name;
        _period = builder._period;
        _operator = builder._operator;
        _value = builder._value;
        _extensions = ImmutableMap.copyOf(builder._extensions);

        _fqdsn = new FQDSN.Builder()
                .setCluster(builder._cluster)
                .setService(builder._service)
                .setMetric(builder._metric)
                .setStatistic(builder._statistic)
                .build();

        final String script =
                "value = ...\n"
                + "return value " + _operator.getToken() + " " + _value.getValue();
        final Globals globals = JsePlatform.standardGlobals();
        _expression = globals.load(script, _name);
    }

    private final String _name;
    private final Period _period;
    private final LuaRelationalOperator _operator;
    private final Quantity _value;
    private final ImmutableMap<String, Object> _extensions;
    private final FQDSN _fqdsn;
    private final LuaValue _expression;

    /**
     * <code>Builder</code> implementation for <code>LuaExpression</code>.
     */
    public static final class Builder extends OvalBuilder<LuaAlert> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(LuaAlert::new);
        }

        /**
         * Set the alert name. Required. Cannot be null or empty.
         *
         * @param value The alert name.
         * @return This <code>Builder</code> instance.
         */
        public Builder setName(final String value) {
            _name = value;
            return this;
        }

        /**
         * Set the cluster. Required. Cannot be null or empty.
         *
         * @param value The cluster.
         * @return This <code>Builder</code> instance.
         */
        public Builder setCluster(final String value) {
            _cluster = value;
            return this;
        }

        /**
         * Set the service. Required. Cannot be null or empty.
         *
         * @param value The service.
         * @return This <code>Builder</code> instance.
         */
        public Builder setService(final String value) {
            _service = value;
            return this;
        }

        /**
         * Set the metric. Required. Cannot be null or empty.
         *
         * @param value The metric.
         * @return This <code>Builder</code> instance.
         */
        public Builder setMetric(final String value) {
            _metric = value;
            return this;
        }

        /**
         * Set the <code>Statistic</code>. Required. Cannot be null.
         *
         * @param value The period.
         * @return This <code>Builder</code> instance.
         */
        public Builder setStatistic(final Statistic value) {
            _statistic = value;
            return this;
        }

        /**
         * Set the period. Required. Cannot be null.
         *
         * @param value The period.
         * @return This <code>Builder</code> instance.
         */
        public Builder setPeriod(final Period value) {
            _period = value;
            return this;
        }

        /**
         * Set the relational operator. Required. Cannot be null.
         *
         * @param value The relational operator.
         * @return This <code>Builder</code> instance.
         */
        public Builder setOperator(final LuaRelationalOperator value) {
            _operator = value;
            return this;
        }

        /**
         * Set the threshold value. Required. Cannot be null.
         *
         * @param value The threshold value.
         * @return This <code>Builder</code> instance.
         */
        public Builder setValue(final Quantity value) {
            _value = value;
            return this;
        }

        /**
         * Set supporting data. Optional. Cannot be null. Default is an empty
         * <code>Map</code>.
         *
         * @param value The supporting data.
         * @return This <code>Builder</code> instance.
         */
        public Builder setExtensions(final Map<String, Object> value) {
            _extensions = value;
            return this;
        }

        @NotNull
        @NotEmpty
        private String _name;
        @NotNull
        @NotEmpty
        private String _cluster;
        @NotNull
        @NotEmpty
        private String _service;
        @NotNull
        @NotEmpty
        private String _metric;
        @NotNull
        private Statistic _statistic;
        @NotNull
        @NotEmpty
        private Period _period;
        @NotNull
        private LuaRelationalOperator _operator;
        @NotNull
        private Quantity _value;
        @NotNull
        private Map<String, Object> _extensions = Collections.emptyMap();
    }
}
