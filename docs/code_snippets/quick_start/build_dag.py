from dagger import dsl

from .quick_start import map_reduce_pipeline

dag = dsl.build(map_reduce_pipeline)
