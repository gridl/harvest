# -*- coding: utf-8 -*-

from datetime import datetime

from django.db import models
from django.utils.translation import ugettext_lazy as _


class Maintainer(models.Model):
    """
    The `Maintainer` class represents a `DataSet` maintainer.
    """
    name = models.CharField(max_length=128,
                           primary_name=True,
                           help_text=_('name of the person or the team'))


class Describable(models.Model):
    """
    `Describable` is an abstract class that represents a common
    interface to objects that are describable.
    """

    class Meta:
        abstract = True

    name = models.CharField(null=False, blank=False, max_length=128)
    description = models.TextField()
    published_on = models.DateTimeField(_('published on'), default=datetime.now)
    modified_on = models.DateTimeField(_('modified on'), default=datetime.now)
    is_active = models.BooleanField(default=True)


class Maintainable(models.Model):
    """
    `Maintainable` is an abstract class that represents a common
    interface to objects that are maintainable.
    """

    class Meta:
        abstract = True

    maintainer = models.ForeignKey(Maintainer, related_name='+')


class DataSet(Maintainable, Describable):
    """
    The `DataSet` class represents a remote storage
    e.g: Hadoop components (like HDFS, Impala, Hive, Spark) and SQL databases (Postgres, etc.)
    """
    POSTGRES = 0
    MYSQL = 1
    VERTICA = 2

    HDFS = 20
    HIVE = 21
    SPARK = 22

    VENDORS = (
        (POSTGRES, 'postgres'),
        (MYSQL, 'mysql'),
        (VERTICA, 'vertica'),
        (HDFS, 'hdfs'),
        (HIVE, 'hive'),
        (SPARK, 'spark'),
    )

    vendor = models.PositiveSmallIntegerField(_('vendor'),
                                             null=False,
                                             blank=False,
                                             choices=VENDORS)
    host = models.CharField(max_length=256)
    port = models.IntegerField()

    def __unicode__(self):
        return '{}<{}>'.format(self.name, self.get_vendor_display())


class Schema(Maintainable, Describable):
    """
    The `Schema` class represents a schema in a `DataSet`.
    """
    dataset = models.ForeignKey(DataSet, related_name='+')

    def __unicode__(self):
        return self.name


class TableDefinition(models.Model):
    """
    The `TableDefinition` class represents a table definition version.
    """
    table = models.ForeignKey(Table)
    create_at = models.DateTimeField(_('created at'), default=datetime.now)
    columns = models.ManyToManyField(Column)
    version_num = models.IntegerField(default=1, editable=False)
    previous = models.ForeignKey('self', blank=True, null=True, editable=False)

    def __unicode__(self):
        return '<{}> version number {}'.format(self.table, self.version_num)

    def save(self, **kwargs):
        """
        Ensures that we always increment the `version_num` counter and setting previous definition.
        """
        # Check if contents have changed... if not, silently ignore save
        if self.table and self.table.definition:
            if self.table.definition.columns == self.columns:
                return

        # Increment `version_num` according to previous revision
        previous_definitions = TableDefinition.objects.filter(table=self.table)
        previous_definitions = previous_definitions.order_by('-version_num')
        count_versions = previous_definitions.count()
        if count_versions > 0:
            latest_definition = previous_definitions.first()
            if count_versions > latest_definition.version_num:
                self.version_num = count_versions + 1
            else:
                self.version_num = latest_definition.version_num + 1
        else:
            self.version_num = 1

        # set the current table definition to be the previous
        self.previous = self.table.defintion
        super(TableDefinition, self).save(**kwargs)

        # update table with the new definition
        self.table.definition = self
        self.table.modified_on = datetime.now
        self.table.save()

    def delete(self, **kwargs):
        """
        If a current definition is deleted, then regress to the previous definition.
        """
        if self.table.definition == self:
            previous = TableDefinition.objects.filter(table=self.table,
                                                      pk__not=self.pk).order_by('-version_num')
            if previous.exists():
                self.table.definition = previous.first()
                self.table.save()
        super(TableDefinition, self).delete(**kwargs)


class Table(Maintainable, Describable):
    """
    The `Table` class represents a table in a `Schema`.
    """
    schema = models.ForeignKey(Schema, related_name='+')
    definition = models.OneToOneField(TableDefinition, blank=True, null=True, editable=True)

    def __unicode__(self):
        return '{}.{}'.format(self.schema, self.name)


class Column(Describable):
    """
    The `Column` class represents a table column.
    """
    data_type = models.CharField(max_length=32)

    def __unicode__(self):
        return self.name
