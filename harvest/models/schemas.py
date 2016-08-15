class SliceSchema(object):
    """
    The `SliceSchema` class represents the schema of the data slice to query.
    """
    def __init__(self, table, metrics, dimensions, filters, group_by, operations):
        self.table = table
        self.metrics = metrics
        self.dimensions = dimensions
        self.filters = filters
        self.group_by = group_by
        self.operations = operations
        self.metric_val_schemas, self.metric_agg_schemas, metric_tables = self._get_metric_schemas()
        self.dimension_schemas, dimension_tables = self._get_dimension_schemas()
        self.filter_schemas, filter_tables = self._get_filter_schemas()

        self.group_by_schema = self._get_group_by_schema()
        self.operation_schemas = self._get_operation_schemas()

        self.used_tables = metric_tables | dimension_tables | filter_tables
        self.joins = [j for j in self.joins or [] if j.table in self.used_tables]

        # assert that the schema representation is correct
        assert (not all([dimensions and group_by]),
                'no dimension is allowed if group by is passed.')
        assert (not all([self.metric_val_schemas, self.metric_agg_schemas]),
                'no metric value is allowed if metric aggregations are passed.')
        assert (not any([self.group_by, self.metric_agg_schemas])
                or all([self.group_by, self.metric_agg_schemas]),
                'no metric aggregation is allowed if no group by is passed.')

        self.query = self._query()

    def _get_metric_schemas(self):
        metric_agg_schemas = [m.schema for m in self.metrics if m.is_aggregation]
        metric_val_schemas = [m.schema for m in self.metrics if not m.is_aggregation]
        metric_tables = {t for m in self.metrics for t in m.tables}
        return metric_val_schemas, metric_agg_schemas, metric_tables

    def _get_dimension_schemas(self):
        dimension_schemas = [d.schema for d in self.dimensions or []]
        dimension_tables = {t for d in self.dimensions or [] for t in d.tables}
        return dimension_schemas, dimension_tables

    def _get_filter_schemas(self):
        filter_schemas = [f.schema for f in self.filters or []]
        filter_tables = {t for f in self.filters or [] for t in f.element.tables}
        return filter_schemas, filter_tables

    def _get_group_by_schema(self):
        return [g.schema for g in self.group_by or []]

    def _get_operation_schemas(self):
        return [o.schema for o in self.operations or []]

    def _add_joins(self, query):
        for join in self.joins:
            query = getattr(query, join.join_type)(join.table.schema, join.criteria)
        return query

    def _add_projections(self, query):
        return query[self.dimension_schemas + self.metric_val_schemas]

    def _add_filters(self, query):
        return query[self.filter_schemas]

    def _add_groupby(self, query):
        return query.group_by(self.group_by)

    def _add_metrics(self, query):
        return query.aggregate(self.metric_agg_schemas)

    def _query(self):
        query = self.table.schema
        query = self._add_joins(query)
        query = self._add_projections(query)
        return self._add_filters(query)
