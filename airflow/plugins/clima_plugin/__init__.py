# openweathermap
from airflow.plugins_manager import AirflowPlugin

#my hook
from clima_plugin.hooks.clima_hook import ClimaHook

#my operator
from clima_plugin.operators.clima_operator import DisplayClimaOperator


class ClimaPlugin(AirflowPlugin):
    name = "ClimaPlugin"  # does not need to match the package name
    operators = [DisplayClimaOperator]
    sensors = []
    hooks = [ClimaHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
    