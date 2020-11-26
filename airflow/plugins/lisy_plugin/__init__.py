# interface Lisy
from airflow.plugins_manager import AirflowPlugin

#my hook
from lisy_plugin.hooks.lisy_hook import LisyHook

#my operator
from lisy_plugin.operators.lisy_operator import LisyQueryCorporateService
from lisy_plugin.operators.lisy_operator import LisyQueryCustom
from lisy_plugin.operators.lisy_operator import LisyCheckTokenOperator
from lisy_plugin.operators.lisy_operator import LisyQueryPort

class LisyPlugin(AirflowPlugin):
    name = "LisyPlugin_v0"  # does not need to match the package name
    operators = [LisyCheckTokenOperator, LisyQueryCorporateService, LisyQueryCustom, LisyQueryPort]
    sensors = []
    hooks = [LisyHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
    