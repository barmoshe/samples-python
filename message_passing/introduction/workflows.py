import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from message_passing.introduction import Language
    from message_passing.introduction.activities import call_greeting_service


@dataclass
class GetLanguagesInput:
    include_unsupported: bool


@dataclass
class ApproveInput:
    name: str


@workflow.defn
class GreetingWorkflow:
    """
    A workflow that that returns a greeting in one of multiple supported
    languages.

    It exposes a query to obtain the current language, a signal to approve the
    workflow so that it is allowed to return its result, and two updates for
    changing the current language and receiving the previous language in
    response.

    One of the update handlers is not an `async def`, so it can only mutate and
    return local workflow state; the other update handler is `async def` and
    executes an activity which calls a remote service, giving access to language
    translations which are not available in local workflow state.
    """

    def __init__(self) -> None:
        self.approved_for_release = False
        self.approver_name: Optional[str] = None
        self.greetings = {
            Language.CHINESE: "你好，世界",
            Language.ENGLISH: "Hello, world",
        }
        self.language = Language.ENGLISH
        self.lock = asyncio.Lock()  # used by the async handler below

    @workflow.run
    async def run(self) -> str:
        # 👉 In addition to waiting for the `approve` Signal, we also wait for
        # all handlers to finish. Otherwise, the Workflow might return its
        # result while an async set_language_using_activity Update is in
        # progress.
        await workflow.wait_condition(
            lambda: self.approved_for_release and workflow.all_handlers_finished()
        )
        return self.greetings[self.language]

    @workflow.query
    def get_languages(self, input: GetLanguagesInput) -> List[Language]:
        # 👉 A Query handler returns a value: it can inspect but must not mutate the Workflow state.
        if input.include_unsupported:
            return sorted(Language)
        else:
            return sorted(self.greetings)

    @workflow.signal
    def approve(self, input: ApproveInput) -> None:
        # 👉 A Signal handler mutates the Workflow state but cannot return a value.
        self.approved_for_release = True
        self.approver_name = input.name

    @workflow.update
    def set_language(self, language: Language) -> Language:
        # 👉 An Update handler can mutate the Workflow state and return a value.
        previous_language, self.language = self.language, language
        return previous_language

    @set_language.validator
    def validate_language(self, language: Language) -> None:
        if language not in self.greetings:
            # 👉 In an Update validator you raise any exception to reject the Update.
            raise ValueError(f"{language.name} is not supported")

    @workflow.update
    async def set_language_using_activity(self, language: Language) -> Language:
        # 👉 This update handler is async, so it can execute an activity.
        if language not in self.greetings:
            # 👉 We use a lock so that, if this handler is executed multiple
            # times, each execution can schedule the activity only when the
            # previously scheduled activity has completed. This ensures that
            # multiple calls to set_language are processed in order.
            async with self.lock:
                greeting = await workflow.execute_activity(
                    call_greeting_service,
                    language,
                    start_to_close_timeout=timedelta(seconds=10),
                )
                # 👉 The requested language might not be supported by the remote
                # service. If so, we raise ApplicationError, which will fail the
                # Update. The WorkflowExecutionUpdateAccepted event will still
                # be added to history. (Update validators can be used to reject
                # updates before any event is written to history, but they
                # cannot be async, and so we cannot use an update validator for
                # this purpose.)
                if greeting is None:
                    raise ApplicationError(
                        f"Greeting service does not support {language.name}"
                    )
                self.greetings[language] = greeting
        previous_language, self.language = self.language, language
        return previous_language

    @workflow.query
    def get_language(self) -> Language:
        return self.language
