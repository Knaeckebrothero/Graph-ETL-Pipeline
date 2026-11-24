"""Scorer core of the schema-fitness technique (Thesis A), version 0.1.

Implements the item table and the cards that can be computed from it or from the run log,
after "Eval 2 package, Technique v0.1", sections 3, 5 and 6. See ../README.md.
"""
from .items import (Item, ItemTable, ItemTableError, ItemType, State, Tier, cq_row, funnel_level,
                    worst_upstream, with_states, FUNNEL_LEVELS)
from .values import (Aggregate, Measure, MIN_DENOMINATOR, K_OF_N, NOT_ESTIMABLE, RATE, item_rate, mean_of,
                     ne_register, wilson)
from . import aggregate, logcards, stability, views

__version__ = "0.1"
