# -*- coding: utf-8 -*-

from django.db import models
from django.utils.translation import ugettext_lazy as _

from .schemas import SliceSchema
from .datasets import Table


class Join(models.Model):
    """
    The `Join` represents a table to join on the main table in a `Slice` instance.
    """
    INNER_JOIN = 1
    CROSS_JOIN = 2
    LEFT_JOIN = 3
    OUTER_JOIN = 4
    SEMI_JOIN = 5
    ANTI_JOIN = 5
    JOIN_TYPES = (
        (INNER_JOIN, 'inner_join'),
        (CROSS_JOIN, 'cross_join'),
        (LEFT_JOIN, 'left_join'),
        (OUTER_JOIN, 'outer_join'),
        (SEMI_JOIN, 'semi_join'),
        (ANTI_JOIN, 'anti_join'),
    )

    table = models.ForeignKey(Table, related_name='table')
    criteria = models.CharField(max_length=1024)
    join_type = models.PositiveSmallIntegerField(_('criterion'),
                                                 null=False,
                                                 choices=JOIN_TYPES,
                                                 default=INNER_JOIN)


class SliceElement(models.Model):
    """
    The `SliceElement` is an abstract class that represents a dimension or a metric.
    """
    class Meta:
        abstract = True

    name = models.CharField(max_length=100)
    label = models.CharField(max_length=256, null=True, blank=True)
    definition = models.TextField(help_text=_('the sql definition of the element.'))
    tables = models.ManyToManyField(Table,
                                    help_text=_('tables needed to query this element.'))
    description = models.TextField()

    def __unicode__(self):
        return self.label


class Dimension(SliceElement):
    """
    The `Dimension` class represents a dimension in the `Slice` object.
    """
    TYPE_BOOL = 0
    TYPE_NUMERIC = 1
    TYPE_DATETIME = 2
    TYPE_CATEGORICAL = 3
    TYPE_UNIQUE = 4

    DATA_TYPES = (
        (TYPE_BOOL, 'boolean'),
        (TYPE_NUMERIC, 'numeric'),
        (TYPE_DATETIME, 'datetime'),
        (TYPE_CATEGORICAL, 'categorical'),
        (TYPE_UNIQUE, 'unique')
    )

    id_definition = models.TextField(help_text=_('the sql definition of the ids.'))
    data_type = models.PositiveSmallIntegerField(_('type'),
                                                 null=False,
                                                 choices=DATA_TYPES)


class Metric(SliceElement):
    """
    The `Metric` class represents a metric in the `Slice` object.
    """
    is_analytic = models.BooleanField(
        default=False,
        help_text=_('indicates that this metric is an analytics function.'))
    is_aggregation = models.BooleanField(
        default=False,
        help_text=_('indicates that this metric is an aggregation and '
                    'that it may require a group by. '
                    'This is also helpful for pre-checking the query.'))


class Slice(models.Model):
    """
    The `Slice` class represents essentially a view on data.
    """
    name = models.CharField(max_length=100)
    description = models.TextField()
    main_table = models.ForeignKey(Table, related_name='slices', related_query_name='slice')
    metrics = models.ManyToManyField(Metric)
    dimensions = models.ManyToManyField(Dimension)
    joins = models.ManyToManyField(Join)

    def _get_schema(self, metrics, dimensions=None, filters=None, group_by=None, operations=None):
        """
        Creates the slicer schema based on the given params.
        :param metrics: list of `Metric` instances.
        :param dimensions: list of `Dimension` instance.
        :param filters: list of `Filter` instances.
        :param group_by: list of `Dimension` instance to group by.
        :param operations: list of operations.
        :return: a `SlicerSchema` instance.
        """

        return SliceSchema(
            table=self.main_table,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            group_by=group_by,
            operations=operations
        )

    @staticmethod
    def _get_display(metrics, dimensions=None, operations=None):
        """
        Creates a map between element names and labels.
        :param metrics: list of `Metric` instances.
        :param dimensions: list of `Dimension` instance.
        :param operations: list of operations.
        :return: a dictionary representing the labels of the data columns.
        """
        return {
            'metrics': {m.name: m.label for m in metrics},
            'dimensions': {d.name: d.label for d in dimensions or []},
            'operations': {o.name: o.label for o in operations or []}
        }

    def data(self, metrics, dimensions=None, filters=None, group_by=None, operations=None):
        """
        Queries the data based on the given params.
        :param metrics: list of `Metric` instances.
        :param dimensions: list of `Dimension` instance.
        :param filters: list of `Filter` instances.
        :param group_by: list of `Dimension` instance to group by.
        :param operations: list of operations.
        :return: a pandas.DataFrame.
        """
        schema = self._get_schema(metrics, dimensions, filters, group_by, operations)
        query = schema.query
        return
