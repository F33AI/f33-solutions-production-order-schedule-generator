import yaml
import uuid
import logging
from pathlib import Path

import streamlit as st
import streamlit_authenticator as stauth
from streamlit_authenticator.utilities.hasher import Hasher
from scheduler import Scheduler

CONFIG_PATH = "config.yaml"
DATASTORE_ENTITY_NAME = "f33-solutions-scheduler"
AUTH_CONFIG = {
    "credentials": {
        "usernames": {}
    },
    "cookie": {
        "expiry_days": 30,
        "key": "b799efae-043d-41ba-bd24-518031428cab",
        "name": "5bbca995-0772-4df2-b6b1-167273dbe55c"
    }
}


def read_yaml_file(path):
    with open(path) as file:
        return yaml.load(file, Loader=yaml.loader.SafeLoader)


def start_session(session, force: bool = False):
    """ It defines a few basic values that sits in the session. The session
        is unique for every visitor. Similar to `gr.State()` but
        created automatically for each visitor.

    Args:
        session (st.session_state): A dictionary that stores values
        force (bool): A flag to force a new session to be created
    """

    if force or not session.get("session_id", False):

        if not Path(CONFIG_PATH).exists():
            print("*** CANNOT LOAD THE CONFIG FILE. EXITING ...***")
            exit(1)

        parameters = read_yaml_file(CONFIG_PATH)
        users = {
            parameters["web_username"]: {
                "name": parameters["web_username"],
                "password": Hasher([parameters["web_password"]]).generate()[0]
            }
        }

        AUTH_CONFIG["credentials"]["usernames"] = users
        authenticator = stauth.Authenticate(
            AUTH_CONFIG["credentials"],
            AUTH_CONFIG["cookie"]["name"],
            AUTH_CONFIG["cookie"]["key"],
            AUTH_CONFIG["cookie"]["expiry_days"],
        )

        session_id = str(uuid.uuid4())
        scheduler = Scheduler(
            project_id=parameters["project_id"],
            region=parameters["region"],
            bucket_name=parameters["bucket_name"],
            entity_name=DATASTORE_ENTITY_NAME,
            artifacts_repository_name=parameters["artifacts_repository_name"],
            batch_machine_type=parameters.get("batch_machine_type", "e2-standard-2")
        )

        session_objects = [
            ("session_id", session_id),
            ("scheduler", scheduler),
            ("new_experiment_kwargs", {}),
            ("scheduler_image_exists", False),
            ("new_experiment_suggested_name", scheduler.get_random_name()),
            ("auth", authenticator)
        ]

        for name, value in session_objects:
            session[name] = value

        logging.info(f"A new session has started ({session_id}).")


def map_job_status_to_text(status: int):
    messages = [":gray[UNSPECIFIED]", ":gray[QUEUED]", ":gray[SCHEDULED]", ":orange[RUNNING]",
                ":green[SUCCEEDED]", ":red[FAILED]", ":red[DELETION_IN_PROGRESS]"]
    return messages[status]


def map_job_status_to_help(status: int):
    messages = [
        ("The status is unspecified. If it won't change in a few minutes "
         "it means that there are some technical issues."),
        "The job is waiting to be allocated on a machine.",
        "The job is currently being allocated on a machine.",
        "The job is running.",
        "The job has succeeded. The results and metrics should be available.",
        "The job couldn't succeeded. Please check the logs to identify the issue.",
        "The job is being deleted."
    ]
    return messages[status]



def inject_css_to_inline_buttons():
    return st.markdown(
        """
        <style>
            div[data-testid="column"] {
                width: fit-content !important;
                flex: unset;
            }
            div[data-testid="column"] * {
                width: fit-content !important;
            }
            div[data-testid="stVerticalBlock"] * {
                gap: 0.5rem !important;
            }
            button {
                color: rgb(163, 168, 184) !important;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

def inject_css_to_center_spinner():
    return st.markdown("""
        <style>
        div.stSpinner > div {
            text-align:center;
            align-items: center;
            justify-content: center;
        }
        </style>""",
        unsafe_allow_html=True
    )

def inject_css_to_center_images():
    return st.markdown(
        """
        <style>
            [data-testid=stSidebar] [data-testid=stImage]{
                text-align: center;
                display: block;
                margin-left: auto;
                margin-right: auto;
                width: 100%;
            }
        </style>
        """,
        unsafe_allow_html=True
    )


def inject_css_to_increase_font_size():
    return st.markdown(
        """
            <style>
                button[data-baseweb="tab"] p {
                    font-size: 17px !important;
                    padding-left: 24px;
                    padding-right: 24px;
                }
            </style>
        """,
        unsafe_allow_html=True
    )


def centered_caption(text: str):
    return st.markdown(
        f"""<div style='width: 100%; font-size: 14px; text-align: center;
                        color: rgb(163, 168, 184);'>
            {text}
        </div>
        """,
        unsafe_allow_html=True
    )