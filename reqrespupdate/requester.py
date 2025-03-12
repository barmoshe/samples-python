import asyncio
import uuid
from temporalio import client
from workflow import Request, UppercaseWorkflowUpdate

class RequesterOptions:
    def __init__(self, client: client.Client, target_workflow_id: str) -> None:
        self.client = client
        self.target_workflow_id = target_workflow_id

class Requester:
    def __init__(self, options: RequesterOptions):
        if options.client is None:
            raise ValueError("Client required")
        if not options.target_workflow_id:
            raise ValueError("Target workflow required")
        self.options = options

    async def request_uppercase(self, text: str) -> str:
        # Create a unique request id.
        req_id = str(uuid.uuid4())
        req = Request(id=req_id, input=text)
        # Get the workflow handle.
        workflow_handle = self.options.client.get_workflow_handle(self.options.target_workflow_id)
        # Send the request signal.
        await workflow_handle.signal(UppercaseWorkflowUpdate.request, req)
        # Poll for a response every 0.5 seconds, up to 20 attempts.
        for _ in range(20):
            response = await workflow_handle.query(UppercaseWorkflowUpdate.get_response, req_id)
            if response:
                return response
            await asyncio.sleep(0.5)
        return ""

async def main():
    temporal_client = await client.Client.connect("localhost:7233")
    options = RequesterOptions(temporal_client, "reqrespupdate_workflow")
    requester = Requester(options)
    
    count = 0
    print("Press Ctrl+C to stop")
    while True:
        text_input = f"foo{count}"
        print(f"Requesting uppercase for: {text_input}")
        result = await requester.request_uppercase(text_input)
        print(f"Response: {result}")
        count += 1
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
