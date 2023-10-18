import os
import json
import streamlit as st
from datetime import time
from datetime import datetime as dt


def load_cfg():
    with open('diary_config.json', 'r') as file:
        data = json.load(file)
        st.session_state.path = data['path']
        st.session_state.tags['activity_category'] = data['activityCategory']
        st.session_state.tags['activity_tags'] = data['activityTags']
        st.session_state.tags['day_tags'] = data['dayTags']


def get_path():
    # Load directory and filename
    filename = f"diary_{st.session_state['date'].strftime('%d%m%Y')}.json"
    filepath = st.session_state["path"] + filename
    return filepath


# Save a day
def save_day():
    # Save the day
    with open(get_path(), 'w') as f:
        json.dump(st.session_state.today, f)


# Load a day
def load_day():
    # Create the directory if it doesn't exist
    if not os.path.exists(st.session_state["path"]):
        os.makedirs(st.session_state["path"])

    # Load directory and filename
    filepath = get_path()

    # Create the file if it doesn't exist
    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            json.dump({
                "date": st.session_state['date'].strftime('%d.%m.%Y'),
                "activity": [],
                "fell_asleep": "00:00",
                "woke_up": "00:00",
                "focus": 5,
                "starting_mood": 5,
                "ending_mood": 5,
                "satisfaction": 5,
                "tags": [],
                "comments": [],
                "changelog": [{"action": "created", "time": dt.now().strftime('%H:%M')}]
            }, f)

    # Load the day
    with open(filepath, 'r') as f:
        st.session_state.today = json.load(f)
        st.session_state.today['changelog'].append(
            {"action": "loaded", "time": dt.now().strftime('%H:%M')})


# Config & initialisation
st.set_page_config(layout="wide", page_title="My Diary",
                   initial_sidebar_state="collapsed", page_icon=":notebook:")

# Check and load tags
if 'tags' or 'path' not in st.session_state:
    st.session_state.tags = {}
    load_cfg()

# Check if the date is in session state
if 'date' not in st.session_state:
    st.session_state['date'] = dt.today()

# Check if today is in session state
if 'today' not in st.session_state or st.session_state['today'] is None:
    load_day()
    print(st.session_state['today']['date'] + " called")

# Set page title
st.title('Today is the {} welcome to your digital diary'.format(
    st.session_state['date'].strftime('%d.%m.%Y')))

# Columns for changing the date and saving the day
col1, col2, col3 = st.columns(3)

# Date input for changing the date
st.session_state.date = col1.date_input(
    "Today's Date", value=st.session_state.date)

# Timestamps for tracking sleep cycle
st.session_state.today['fell_asleep'] = col2.time_input(
    'Slept from', key='sleep', value=dt.strptime(
        st.session_state.today['fell_asleep'], '%H:%M').time()).strftime('%H:%M')
st.session_state.today['woke_up'] = col3.time_input(
    'Slept until', key='wake', value=dt.strptime(
        st.session_state.today['woke_up'], '%H:%M').time()).strftime('%H:%M')

# Split application into tabs
activities, rate, write = st.tabs(
    ["What did you do today?",
     "Rate your day!",
     "Wanna mention something?"])

# Activities section
with activities:
    if 'activity_start' not in st.session_state:
        st.session_state.activity_category_default = 2
        st.session_state.activity_description = ""
        st.session_state.activity_start = time(0, 0)
        st.session_state.activity_end = time(0, 0)
        st.session_state.activity_tags = []

    # Specify how demanding the activity was.
    st.session_state.activity_category = st.selectbox(
        'Tag based on cognitive demand',
        st.session_state.tags['activity_category'],
        key='activity_category_widget', index=st.session_state.activity_category_default)

    # Specify the activity name.
    st.session_state.activity_description = st.text_input(
        label='Describe your activity',
        key='activity_description_widget',
        placeholder='What did you do?',
        value=st.session_state.activity_description)

    # Specify the start and end time of the activity.
    st.session_state.activity_start = st.time_input(
        'Start time', key='activity_start_widget', value=st.session_state.activity_start)
    st.session_state.activity_end = st.time_input(
        'End time', key='activity_end_widget', value=st.session_state.activity_end)

    # Tag your activity
    st.session_state.activity_tags = st.multiselect(
        "Activity was related to",
        options=st.session_state.tags['activity_tags'],
        key='activity_tags_widget', default=st.session_state.activity_tags)

    # Add column for button and last activity
    col1, col2 = st.columns(2)

    # Button to add a new activity
    if col1.button("Add activity"):
        # Append activity to the list
        st.session_state.today["activity"].append(
            {"description": st.session_state['activity_description'],
             "start": st.session_state['activity_start'].strftime('%H:%M'),
             "end": st.session_state['activity_end'].strftime('%H:%M'),
             "category": st.session_state['activity_category'],
             "tags": st.session_state['activity_tags']})

        # Reset activity input fields
        st.session_state.activity_category_default = 3
        st.session_state.activity_description = ""
        st.session_state.activity_start = st.session_state['activity_end']
        st.session_state.activity_end = time(0, 0)
        st.session_state.activity_tags = []
        st.rerun()

    # Last activity
    with col2:
        activity_len = len(st.session_state.today["activity"])
        if activity_len > 0:
            previous_activity = 'Previous activity was: {} | ended: {}'.format(
                st.session_state.today["activity"][activity_len - 1]["description"],
                st.session_state.today["activity"][activity_len - 1]["end"])
        else:
            previous_activity = "No previous activity"
        st.text(previous_activity)

    important_habit = st.text_input(label='Keep track and test new stuff!',
                                    placeholder='Could be anything like a habit or food supplements...')
    if st.button("Mark today"):
        # Append comment to the list
        st.session_state.today["comments"].append(
            {"type": "Important Comment",
             "time": dt.now().strftime('%H:%M'),
             "text": important_habit})

        # Reset comment input field
        important_habit = ""

# Rate section
with rate:
    # Sliders and diagram
    col1, col2 = st.columns(2)

    # Sliders used to judge different activities
    with col1:
        # Subtitle and description
        st.header("How would you judge your...")

        # Sliders
        st.session_state.today["focus"] = st.slider(
            "Ability to stay focused throughout the day.", 1, 10,
            value=st.session_state.today["focus"])
        st.session_state.today["starting_mood"] = st.slider(
            "Mood at the start of the day.", 1, 10,
            value=st.session_state.today["starting_mood"])
        st.session_state.today["ending_mood"] = st.slider(
            "Mood at the end of the day.", 1, 10,
            value=st.session_state.today["ending_mood"])
        st.session_state.today["satisfaction"] = st.slider(
            "Satisfaction with what you have achieved today.", 1, 10,
            value=st.session_state.today["satisfaction"])

    with col2:
        # Subtitle and description
        st.header("Anything you wanna mention?")

        # Specify what you want to comment on.
        st.session_state.comment_category = st.selectbox(
            'Tag based on cognitive demand',
            ['Focus',
             'Morning Mood',
             'Evening Mood',
             'Satisfaction'],
            key='comment_category_widget')
        st.session_state.rate_comment = st.text_input("Do you wanna comment on any of the sliders?",
                                                      key='rate_comment_widget')

        # Button to add a comment
        if st.button("Add comment"):
            # Append activity to the list
            st.session_state.today["comments"].append(
                {"type": st.session_state['comment_category'],
                 "time": dt.now().strftime('%H:%M'),
                 "text": st.session_state['rate_comment']})
            st.rerun()

    # Subtitle and description
    st.header("Tag your day using these!")

    # Checkboxes for tagging the day.
    st.session_state.today["tags"] = st.multiselect(
        "Tag your day!", label_visibility='collapsed', help="Select all that apply",
        options=st.session_state.tags['day_tags'], key='tags_widget',
        default=st.session_state.today['tags'])

# Write section
with write:
    st.header("Is there anything you wanna mention or comment on?")
    st.session_state.main_comment = st.text_area(
        label="Write here!", label_visibility='hidden',
        height=350, placeholder="Heute habe ich...")

    # Checkboxes for tagging today's comment as important.
    if st.checkbox("Contains important information"):
        st.session_state.main_important_comment = True
    else:
        st.session_state.main_important_comment = False

    # Button to add a comment
    if st.button("Comment today"):
        temp_name = "Important Comment" if st.session_state.main_important_comment else "Comment"

        # Append comment to the list
        st.session_state.today["comments"].append(
            {"type": temp_name,
             "time": dt.now().strftime('%H:%M'),
             "text": st.session_state['main_comment']})

        # Reset comment input field
        st.session_state.main_comment = ""
        st.session_state.main_important_comment = False
        st.rerun()

if st.session_state['date'].strftime('%d.%m.%Y') != st.session_state.today['date']:
    # Delete today from st.session_state and rerun script to create or load the new day
    load_day()
    st.rerun()
else:
    # Save changes to file on script rerun
    save_day()
