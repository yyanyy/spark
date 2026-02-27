/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

/**
 * A mix in interface for {@link Scan}. Data sources can implement this interface to
 * report statistics to Spark.
 * <p>
 * As of Spark 3.0, statistics are reported to the optimizer after operators are pushed to the
 * data source. Implementations may return more accurate statistics based on pushed operators
 * which may improve query performance by providing better information to the optimizer.
 * <p>
 * When filters are pushed to the data source, the returned {@link Statistics#numRows()} should
 * reflect the post-filter row count (i.e., the estimated number of rows after pushed predicates
 * are applied). Column statistics such as min, max, and null count may also be updated to reflect
 * the pruned data.
 * <p>
 * However, non-decomposable column statistics like NDV (number of distinct values) typically
 * cannot be accurately recomputed after filter pushdown and often remain at their full-table
 * values. This can cause the optimizer to underestimate the output of NDV-dependent filters
 * (such as equality and IN predicates) when applied on top of already-pruned statistics. To
 * address this, data sources should also implement {@link Statistics#numRowsBeforeFilters()} to
 * report the pre-filter total row count, enabling the optimizer to correctly rescale NDV-based
 * selectivity estimates.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsReportStatistics extends Scan {

  /**
   * Returns the estimated statistics of this data source scan.
   */
  Statistics estimateStatistics();
}
