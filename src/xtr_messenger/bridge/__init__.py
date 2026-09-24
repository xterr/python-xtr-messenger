"""Transports that need a driver.

Kept apart from :mod:`xtr_messenger.transport` because everything there
imports with no optional dependency installed. A bridge does not: it is
reachable only once its extra is present, which is what lets an application
that speaks one transport avoid paying for the others.
"""
