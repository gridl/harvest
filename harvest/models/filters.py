# -*- coding: utf-8 -*-


class Filter(object):
    def __init__(self, slice_element):
        self.slice_element = slice_element


class ComparisonFilter(Filter):
    eq = 'eq'
    ne = 'ne'
    gt = 'gt'
    lt = 'lt'
    gte = 'gte'
    lte = 'lte'
    like = 'like'

    def __init__(self, slice_element, operator, value):
        super(ComparisonFilter, self).__init__(slice_element)
        self.operator = operator
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, ComparisonFilter):
            return False

        return (self.slice_element.name == other.slice_element.name and
                self.operator == other.operator and
                self.value == other.value)

    def __hash__(self):
        return hash('{}_{}_{}'.format(self.slice_element.name, self.operator, self.value))


class ContainsFilter(Filter):
    def __init__(self, slice_element, values):
        super(ContainsFilter, self).__init__(slice_element)
        self.values = values

    def __eq__(self, other):
        if not isinstance(other, ContainsFilter):
            return False

        return self.slice_element.name == other.slice_element.name and self.values == other.values

    def __hash__(self):
        return hash('{}_{}'.format(self.slice_element.name, self.values))


class RangeFilter(Filter):
    def __init__(self, slice_element, start, stop):
        super(RangeFilter, self).__init__(slice_element)
        self.start = start
        self.stop = stop

    def __eq__(self, other):
        if not isinstance(other, RangeFilter):
            return False

        return (self.slice_element.name == other.slice_element.name and
                self.start == other.start and
                self.stop == other.stop)

    def __hash__(self):
        return hash('{}_{}_{}'.format(self.slice_element.name, self.start, self.stop))
