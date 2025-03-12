from datetime import timedelta
from dataclasses import dataclass, field
from temporalio import workflow, activity
import asyncio

# Parameters for starting or continuing the workflow.
@dataclass
class UppercaseParams:
    initial_message: str
    previous_responses: dict[str, str] = field(default_factory=dict)

# Data classes for message passing.
@dataclass
class Request:
    id: str
    input: str

@dataclass
class Response:
    output: str

# Activity: Uppercase the provided string.
@activity.defn
async def uppercase_activity(text: str) -> str:
    return text.upper()

@workflow.defn
class UppercaseWorkflowUpdate:
    @workflow.init
    def __init__(self, params: UppercaseParams) -> None:
        # Initialize workflow state.
        self.requests_buffer: list[Request] = []
        # Load any previous responses from parameters.
        self.responses_map: dict[str, str] = params.previous_responses.copy() if params.previous_responses else {}
        self.request_count: int = 0
        self.pending_updates: int = 0
        # When this many requests are processed, we trigger continue-as-new.
        self.threshold_for_continue: int = 5
        self.initial_message: str = params.initial_message

    @workflow.signal
    async def request(self, req: Request) -> None:
        # Enqueue an incoming request.
        self.requests_buffer.append(req)

    @workflow.update
    async def set_response(self, req_id: str, result: str) -> bool:
        # Store the result of processing the request.
        self.responses_map[req_id] = result
        return True

    @workflow.query
    def get_response(self, req_id: str) -> str:
        # Return the stored response for the given request id.
        return self.responses_map.get(req_id, "")

    @workflow.run
    async def run(self, params: UppercaseParams) -> None:
        # Use the initial parameter from params.
        self.initial_message = params.initial_message

        while True:
            try:
                # Wait deterministically for up to 1 second for a new request.
                await workflow.wait_condition(lambda: len(self.requests_buffer) > 0)
            except TimeoutError:
                # Timeout is acceptable; continue looping.
                pass

            # Process all pending requests.
            while self.requests_buffer:
                req = self.requests_buffer.pop(0)
                self.request_count += 1
                self.pending_updates += 1
                try:
                    result = await workflow.execute_activity(
                        uppercase_activity,
                        req.input,
                        schedule_to_close_timeout=timedelta(seconds=5)
                    )
                    await self.set_response(req.id, result)
                except Exception as err:
                    workflow.logger.error(f"Error processing request {req.id}: {err}")
                finally:
                    self.pending_updates -= 1

            # If we've processed enough requests and no updates are pending, trigger continue-as-new.
            if self.request_count >= self.threshold_for_continue and self.pending_updates == 0:
                workflow.logger.info("Triggering continue-as-new after processing sufficient updates")
                # Pass current responses along so that they aren't lost.
                new_params = UppercaseParams(
                    initial_message="",
                    previous_responses=self.responses_map.copy(),
                )
                await workflow.continue_as_new(new_params)
                # Execution of the current run ends here.
