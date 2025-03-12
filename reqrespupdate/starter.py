import asyncio
from temporalio import client
from workflow import UppercaseWorkflowUpdate, UppercaseParams

async def main():
    temporal_client = await client.Client.connect("localhost:7233")
    workflow_handle = await temporal_client.start_workflow(
        UppercaseWorkflowUpdate.run,
        UppercaseParams(initial_message=""),  # Pass the parameter; no default.
        id="reqrespupdate_workflow",
        task_queue="reqrespupdate",
    )
    print("Started workflow with ID:", workflow_handle.id)

if __name__ == "__main__":
    asyncio.run(main())
