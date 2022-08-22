from dataclasses import dataclass

from typing import Optional

from prefect.client import Secret 
from prefect.engine.state import State
from prefect import Flow

import pymsteams


@dataclass
class FLA_Teams:

    webhook: str = Secret("TEAMS_NOTIFICATION_WEBHOOK_URL").get()


    def __post_init__(self):

        self.my_message = pymsteams.connectorcard(self.webhook)


    def send_message(self, body_text: str) -> None:

        self.my_message.text(body_text)
        self.my_message.send()

        return None

    def teams_flow_state_handler(self, flow: Flow, old_state: State, new_state: State) -> Optional[State]:

        if new_state.is_failed():

            self.send_message(
                f"""Flow: _{flow.name}_ FAILED!
                {chr(10)} {new_state.result}
                """
            )

        return new_state