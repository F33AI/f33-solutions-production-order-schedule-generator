import json
import content
import streamlit as st
import helpers as sth
from streamlit_autorefresh import st_autorefresh

# -- [ Pre-work ] ---
st.set_page_config(page_title="Factory Schedules Optimizer / F33.ai", layout="wide")
sth.inject_css_to_increase_font_size()
sth.inject_css_to_center_spinner()

with st.spinner("Creating a new session ..."):
    sth.start_session(st.session_state)

with st.columns(3)[1]:
    name, authentication_status, username = st.session_state.auth.login()

if authentication_status == False:
    with st.columns(3)[1]:
        st.error("Username/password is incorrect")

elif authentication_status == None:
    pass

else:

    # -- [ Content ] ---
    logo, header = st.columns([0.12, 0.88])
    with logo:
        st.image("web/static/logo_300.png")

    with header:
        st.title("Factory Schedules Optimizer")
        st.caption("A basic framework for generating and comparing factory production schedules in a efficient way.")
        st.session_state.auth.logout()

    st.write("##")

    if not st.session_state.scheduler_image_exists:
        if st.session_state.scheduler.check_if_scheduler_image_exists():
            st.session_state.scheduler_image_exists = True
        else:
            with st.spinner("Preparing solver code ..."):
                sth.centered_caption("It's one-time only operation. It should take no more than a few minutes.")
                st.session_state.scheduler.build_scheduler_container()
                st.rerun()

    create_a_new_experiment, check_results = st.tabs(
        ["Create a new experiment", "Check results"]
    )

    # -------------------------------
    #       CREATE A NEW EXPERIMENT
    # -------------------------------
    with create_a_new_experiment:

        if st.session_state.new_experiment_kwargs:
            with st.spinner("Creating a new experiment ..."):
                kwargs = st.session_state.new_experiment_kwargs
                st.session_state.scheduler.run_experiment(**kwargs)
                st.session_state.new_experiment_kwargs = {}
                st.session_state.new_experiment_suggested_name = \
                    st.session_state.scheduler.get_random_name()

        with st.form("New experiment", clear_on_submit=True,
                    border=False):


            st.header("Flexible job shop problem")
            exp_name = st.text_input("Experiment name:",
                                    value=st.session_state.new_experiment_suggested_name,
                                    placeholder="Use a meaningful name for your experiment...")


            jobs_json_col, scenarios_json_col = st.columns(2)
            with jobs_json_col:
                st.markdown("## Defined Jobs")
                st.markdown(content.JOBS_DESC)
                with st.expander("Required data structure with examples"):
                    st.markdown("The JSON needs to have the following columns defined (all values are mandatory):")
                    st.code(content.JOBS_STRUCTURE)
                    st.markdown("Example submission:")
                    st.code(content.JOBS_EXAMPLE)

                use_example_jobs = st.toggle("Use the jobs from the example", key="key_use_example_jobs")
                jobs_json_file = st.file_uploader("Choose jobs.json", disabled=use_example_jobs,
                                                type="json", label_visibility="collapsed")

            with scenarios_json_col:
                st.markdown("## Scenarios")
                st.markdown(content.SCENARIOS_DESC)
                with st.expander("Required data structure with examples"):
                    st.markdown("The CSV needs to have the following columns defined (all values are mandatory):")
                    st.code(content.SCENARIOS_STRUCTURE)
                    st.markdown("Example submission:")
                    st.code(content.SCENARIOS_EXAMPLE)

                use_example_scenario = st.toggle("Use the parameters from the example",
                                                key="key_use_example_scenario")
                scenarios_csv_file = st.file_uploader("Choose scenarios.csv",
                                                    type="csv", label_visibility="collapsed")

            st.write("##")
            if st.form_submit_button("Run", type="primary", use_container_width=True):

                with st.spinner("Creating a new experiment ..."):
                    try:
                        b_jobs = jobs_json_file.getvalue() if not st.session_state.key_use_example_jobs else \
                            str.encode(content.JOBS_EXAMPLE)
                        b_scenarios = scenarios_csv_file.getvalue() if not st.session_state.key_use_example_scenario else \
                            str.encode(content.SCENARIOS_EXAMPLE)

                        st.session_state.new_experiment_kwargs = dict(
                            experiment_name=exp_name,
                            jobs=b_jobs,
                            scenarios=b_scenarios
                        )
                        st.rerun()

                    except AttributeError:
                        st.warning("Please provide input files (or use examples) to run the experiment.")


    # -------------------------------
    #      CHECK PROGRESS
    # -------------------------------
    with check_results:

        if st.button("Refresh", use_container_width=True):
            st.rerun()

        if len(st.session_state.scheduler.experiments) == 0:
            st.info("Please create an experiment first.")
        else:
            for experiment in st.session_state.scheduler.experiments[::-1]:

                st.header(f"Experiment: :orange[{experiment.experiment_name}]", divider=True)
                st.markdown("##### Scenarios:")

                with st.container(border=True):
                    for scenario in experiment.scenarios:
                        status_text = sth.map_job_status_to_text(scenario.status)
                        help_text = sth.map_job_status_to_help(scenario.status)
                        columns = st.columns([0.3, 0.1, 0.15, 0.15, 0.15, 0.15])
                        items = [
                            (st.markdown, scenario.scenario_name, {"help": f"JobID: {scenario.batch_job_name}"}),
                            (st.markdown, status_text, {"help": help_text}),
                        ]
                        for column, (method, param, kwargs) in zip(columns[:2], items):
                            with column:
                                method(param, **kwargs)

                        with columns[2]:
                            with st.popover("Parameters", use_container_width=True):
                                st.code(json.dumps(scenario.params, indent=4))

                        with columns[3]:
                            url = st.session_state.scheduler.get_artifacts_url(scenario)
                            st.link_button("Artifacts", url, use_container_width=True)

                        with columns[4]:
                            url = st.session_state.scheduler.get_logs_url(scenario)
                            st.link_button("Logs", url, use_container_width=True,
                                        disabled=scenario.status < 3)

                        with columns[5]:
                            with st.popover("Metrics", use_container_width=True,
                                            disabled=scenario.status != 4):
                                st.code(json.dumps(scenario.metrics, indent=4))

                st.markdown("##### Charts:")
                gantt_col, radar_col = st.columns(2)

                with gantt_col:
                    with st.expander("Review Gantt charts"):
                        options = [scenario.scenario_name for scenario in experiment.scenarios
                                if scenario.status == 4]

                        if len(options):
                            selected_scenario_name = st.selectbox("Choose one of the scenarios", options,
                                                                key=f"select_{experiment.experiment_name}")

                            selected_scenario = None
                            for scenario in experiment.scenarios:
                                if scenario.scenario_name == selected_scenario_name:
                                    selected_scenario = scenario
                                    break

                            if selected_scenario is None:
                                st.warning("Technical error. Cannot find scenario by name.")
                            else:
                                if len(selected_scenario.results) == 0:
                                    st.warning(
                                        "There are no results for this scenario! "
                                        "Probably there are no feasible solution for the given parameters."
                                    )
                                else:
                                    st.plotly_chart(st.session_state.scheduler.render_gantt_chart(selected_scenario))
                        else:
                            st.info("There are no finished scenarios.")

                with radar_col:
                    with st.expander("Compare all plans on a radar plot"):

                        if experiment.status != 4:
                            st.warning("Scenarios that are still running or have failed won't be displayed.")

                        statuses = [len(scenario.results) == 0 and experiment.status == 4
                            for scenario in experiment.scenarios]

                        if all(statuses):
                            st.warning("Cannot find feasible solution for the given problem.")
                        else:
                            st.plotly_chart(st.session_state.scheduler.render_radar_plot(experiment))

                export_col, delete_col = st.columns([0.8, 0.2])

                with delete_col:
                    if st.button("Delete", key=f"delete_btn_{experiment.experiment_name}",
                                use_container_width=True):
                        st.session_state.scheduler.delete_experiment(experiment)
                        st.rerun()

                st.markdown("##")

    sth.centered_caption("Created by <a href='https://f33.ai'>F33.AI</a> | 2024")
