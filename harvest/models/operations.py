# -*- coding: utf-8 -*-


class Operation(object):
    def __init__(self, element, name, label):
        self.element = element
        self.name = name
        self.label = label


class WoW(Operation):
    def __init__(self, element):
        super(WoW, self).__init__(element,
                                  'wow_{}'.format(element.name),
                                  'WoW({})'.format(element.label))


class MoM(Operation):
    def __init__(self, element):
        super(MoM, self).__init__(element,
                                  'mom_{}'.format(element.name),
                                  'MoM({})'.format(element.label))


class YoY(Operation):
    def __init__(self, element):
        super(YoY, self).__init__(element,
                                  'yoy_{}'.format(element.name),
                                  'YoY({})'.format(element.label))
