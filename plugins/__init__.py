'''
from __future__ import division, absolute_import, print_function
import sys
from airflow.plugins_manager import AirflowPlugin
from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator

# Defining the plugin class
class zlPlugin(AirflowPlugin):
    name = "zlPlugin"
    # A list of class(es) derived from BaseOperator
    operators = [
        DataQualityOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        StageToRedshiftOperator
    ]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []

'''



