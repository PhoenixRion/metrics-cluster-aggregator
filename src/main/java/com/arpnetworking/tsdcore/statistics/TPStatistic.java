/**
 * Copyright 2014 Brandon Arp
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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for percentile based statistics.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public abstract class TPStatistic extends BaseStatistic implements OrderedStatistic {

    /**
     * Accessor for the percentile from 0 to 100 (inclusive).
     *
     * @return The percentile.
     */
    public double getPercentile() {
        return _percentile;
    }

    @Override
    public String getName() {
        return _defaultName;
    }

    @Override
    public Set<String> getAliases() {
        return Collections.unmodifiableSet(_aliases);
    }

    @Override
    public Calculator<Void> createCalculator() {
        return new PercentileCalculator(this);
    }

    @Override
    public Set<Statistic> getDependencies() {
        return DEPENDENCIES.get();
    }

    @Override
    public Quantity calculate(final List<Quantity> orderedValues) {
        final int index = (int) (Math.ceil((_percentile / 100.0) * (orderedValues.size() - 1)));
        return orderedValues.get(index);
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        final List<Quantity> allSamples = Lists.newArrayList();
        for (final AggregatedData aggregation : aggregations) {
            allSamples.addAll(aggregation.getSamples());
        }
        Collections.sort(allSamples);
        final int index = (int) (Math.ceil((_percentile / 100.0) * (allSamples.size() - 1)));
        return allSamples.get(index);
    }

    /**
     * Protected constructor.
     *
     * @param percentile The percentile value to compute.
     */
    protected TPStatistic(final double percentile) {
        _percentile = percentile;
        _defaultName = "tp" + FORMAT.format(_percentile);
        _aliases = Sets.newHashSet();
        _aliases.add(_defaultName);
        _aliases.add(_defaultName.substring(1));
        _aliases.add(_defaultName.replace(".", "p"));
        _aliases.add(_defaultName.substring(1).replace(".", "p"));
    }

    private final double _percentile;
    private final String _defaultName;
    private final Set<String> _aliases;

    private static final DecimalFormat FORMAT = new DecimalFormat("##0.#");
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Supplier<Statistic> HISTOGRAM_STATISTIC =
            Suppliers.memoize(() -> STATISTIC_FACTORY.getStatistic("histogram"));
    private static final Supplier<Set<Statistic>> DEPENDENCIES =
            Suppliers.memoize(() -> ImmutableSet.of(HISTOGRAM_STATISTIC.get()));
    private static final long serialVersionUID = 2002333257077042351L;

    /**
     * Calculator computing the percentile of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class PercentileCalculator extends BaseCalculator<Void> {

        /**
         * Public constructor.
         *
         * @param statistic The <code>TPStatistic</code>.
         */
        public PercentileCalculator(final TPStatistic statistic) {
            super(statistic);
        }

        @Override
        public CalculatedValue<Void> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            final HistogramStatistic.HistogramAccumulator calculator =
                    (HistogramStatistic.HistogramAccumulator) dependencies.get(HISTOGRAM_STATISTIC.get());
            return new CalculatedValue.Builder<Void>()
                    .setValue(calculator.calculate(((TPStatistic) getStatistic()).getPercentile()))
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), getStatistic());
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof PercentileCalculator)) {
                return false;
            }

            final PercentileCalculator otherPercentileCalculator = (PercentileCalculator) other;
            return getStatistic().equals(otherPercentileCalculator.getStatistic());
        }
    }
}
