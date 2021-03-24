from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
# import helpers

# Defining the plugin class
class UdacityAirflow(AirflowPlugin):
    name = "udacity_dend_plugin"
    operators = [
        operators.StageToRedshiftOperator
        # operators.LoadFactOperator,
        # operators.LoadDimensionOperator,
        # operators.DataQualityOperator
    ]
    # helpers = [
    #     helpers.SqlQueries
    # ]
