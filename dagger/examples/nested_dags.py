import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger import DAG, DAGOutput, Node


def brainstorm_themes():
    return {
        "first": "love",
        "second": "loss",
    }


def compose_song(theme: str, style: str) -> str:
    return f"{style} song about {theme}"


def record_song(composition: str) -> str:
    return f"recording of ({composition})"


def publish_album(album_name: str, first_song: str, second_song: str) -> dict:
    return {
        "name": album_name,
        "tracks": [first_song, second_song],
    }


def compose_and_record_song(theme, style):
    return DAG(
        {
            "compose": Node(
                compose_song,
                inputs={
                    "theme": inputs.FromParam(),
                    "style": inputs.FromParam(),
                },
                outputs={
                    "composition": outputs.FromReturnValue(),
                },
            ),
            "record": Node(
                record_song,
                inputs={
                    "composition": inputs.FromNodeOutput("compose", "composition"),
                },
                outputs={
                    "recording": outputs.FromReturnValue(),
                },
            ),
        },
        inputs={
            "theme": theme,
            "style": style,
        },
        outputs={"song": DAGOutput("record", "recording")},
    )


dag = DAG(
    nodes={
        "brainstorm-themes": Node(
            brainstorm_themes,
            outputs={
                "first_theme": outputs.FromKey("first"),
                "second_theme": outputs.FromKey("second"),
            },
        ),
        "record-first-song": compose_and_record_song(
            theme=inputs.FromNodeOutput("brainstorm-themes", "first_theme"),
            style=inputs.FromParam(),
        ),
        "record-second-song": compose_and_record_song(
            theme=inputs.FromNodeOutput("brainstorm-themes", "second_theme"),
            style=inputs.FromParam(),
        ),
        "publish-album": Node(
            publish_album,
            inputs={
                "album_name": inputs.FromParam(),
                "first_song": inputs.FromNodeOutput("record-first-song", "song"),
                "second_song": inputs.FromNodeOutput("record-second-song", "song"),
            },
            outputs={
                "album": outputs.FromReturnValue(),
            },
        ),
    },
    inputs={
        "album_name": inputs.FromParam(),
        "style": inputs.FromParam(),
    },
    outputs={
        "album": DAGOutput("publish-album", "album"),
    },
)


def run_from_cli():
    from dagger.runtime.cli import invoke

    invoke(dag)
