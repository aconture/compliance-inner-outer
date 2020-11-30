# interface Lisy
from airflow.plugins_manager import AirflowPlugin

#my operator
from teco_ansible.operators.tecoAnsibleOperator import tecoCallAnsible

class TecoAnsiblePlugin(AirflowPlugin):
    name = "TEcoAnsiblePlugin_v0"  # does not need to match the package name
    operators = [tecoCallAnsible]
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
    