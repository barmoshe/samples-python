import asyncio
from temporalio import client, worker
from workflow import UppercaseWorkflowUpdate, uppercase_activity

async def main():
    temporal_client = await client.Client.connect("localhost:7233")
    task_queue = "reqrespupdate"
    worker_instance = worker.Worker(
        temporal_client,
        task_queue=task_queue,
        workflows=[UppercaseWorkflowUpdate],
        activities=[uppercase_activity],
    )
    print("Worker started on task queue:", task_queue)
    await worker_instance.run()

if __name__ == "__main__":
    asyncio.run(main())
