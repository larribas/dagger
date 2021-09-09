"""
# Nested DAGs.

This example showcases how multiple dags can be composed together.

In a real setting, you can use this feature to modularize your workflow definitions and keep each unit simple, testable and reusable.


## Behavior

Say we want to model a workflow that composes, records and publishes a music album.

The first step will be to brainstorm a few themes, and come up with a mapping of tracks -> themes.

Next, each of the tracks will go through a long process that involves composing the track, arranging it and recording it. Since this is a fairly complex process in and of itself, and we will want to repeat it multiple times during the recording of the album, we abstract it into a separate DAG.

Finally, when all tracks have been independently recorded, we want to put them together and publish the album.


## Implementation

Notice how we used a Python function to generate a `compose_and_record_song` DAG. We want to supply the themes we brainstormed before as inputs to the DAG. We will only know what those inputs are once there is a brainstorming session, so we pass them as arguments to the function.

Now, we can invoke the `compose_and_record_song` function multiple times in the parent DAG: one time for each track we want to record. There, we link the inputs of the inner DAG to the outputs of the brainstorming task.

Finally, we connect the "publish-album" task to the outputs of the inner DAGs.
"""

from typing import Mapping

from dagger import DAG, Task
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromReturnValue


def brainstorm_themes() -> Mapping[str, str]:  # noqa
    themes = {
        "first": "love",
        "second": "loss",
    }
    print(f"Themes: {themes}")
    return themes


def compose_song(theme: str, style: str) -> str:  # noqa
    composition = f"{style} song about {theme}"
    print(f"Composition: {composition}")
    return composition


def record_song(composition: str) -> str:  # noqa
    recording = f"recording of ({composition})"
    print(f"Recording: {recording}")
    return recording


def publish_album(album_name: str, first_song: str, second_song: str) -> dict:  # noqa
    album = {
        "name": album_name,
        "tracks": [first_song, second_song],
    }
    print(f"Album: {album}")
    return album


def compose_and_record_song(theme, style):  # noqa
    return DAG(
        {
            "compose": Task(
                compose_song,
                inputs={
                    "theme": FromParam(),
                    "style": FromParam(),
                },
                outputs={
                    "composition": FromReturnValue(),
                },
            ),
            "record": Task(
                record_song,
                inputs={
                    "composition": FromNodeOutput("compose", "composition"),
                },
                outputs={
                    "recording": FromReturnValue(),
                },
            ),
        },
        inputs={
            "theme": theme,
            "style": style,
        },
        outputs={"song": FromNodeOutput("record", "recording")},
    )


dag = DAG(
    nodes={
        "brainstorm-themes": Task(
            brainstorm_themes,
            outputs={
                "first_theme": FromKey("first"),
                "second_theme": FromKey("second"),
            },
        ),
        "record-first-song": compose_and_record_song(
            theme=FromNodeOutput("brainstorm-themes", "first_theme"),
            style=FromParam(),
        ),
        "record-second-song": compose_and_record_song(
            theme=FromNodeOutput("brainstorm-themes", "second_theme"),
            style=FromParam(),
        ),
        "publish-album": Task(
            publish_album,
            inputs={
                "album_name": FromParam(),
                "first_song": FromNodeOutput("record-first-song", "song"),
                "second_song": FromNodeOutput("record-second-song", "song"),
            },
            outputs={
                "album": FromReturnValue(),
            },
        ),
    },
    inputs={
        "album_name": FromParam(),
        "style": FromParam(),
    },
    outputs={
        "album": FromNodeOutput("publish-album", "album"),
    },
)


if __name__ == "__main__":
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
