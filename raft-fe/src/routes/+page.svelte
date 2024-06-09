<script lang="ts">
  import "../app.css";
  import { Label, Input, P, Spinner, Heading } from "flowbite-svelte";
  import { AngleRightOutline } from "flowbite-svelte-icons";

  interface Entry {
    command: string;
    term: number;
  }

  const address = `${import.meta.env.VITE_A_HOST}:${import.meta.env.VITE_A_PORT}`;

  let command = "";

  let output: string[] = [];

  let loading = false;

  async function onEnter(event: KeyboardEvent) {
    if (event.key !== "Enter") {
      return;
    }

    loading = true;

    let formBody = `command=${command}`;

    const response = await fetch(`http://${address}/command`, {
      method: "POST",
      body: formBody,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    });

    let data = await response.text();
    if (command == "request log") {
      const unmarshal: Entry[] = JSON.parse(data);

      for (let i = unmarshal.length - 1; i >= 0; i--) {
        data = `    ${unmarshal[i].term} - ${unmarshal[i].command}\n`;
        output.unshift(data);
      }

      output.unshift("Log Entry:\n");
    } else {
      output.unshift(data);
    }

    output = output;

    loading = false;
  }
</script>

<div class="m-16 flex flex-col gap-y-8">
  <Label class="space-y-2">
    <Heading tag="h6" class="mb-4">Type your command below</Heading>

    <div class="flex flex-row gap-x-8">
      <Input
        type="email"
        placeholder="ping"
        size="lg"
        bind:value={command}
        on:keydown={onEnter}
        disabled={loading}
      >
        <AngleRightOutline slot="left" class="w-6 h-6" />
      </Input>

      {#if loading}
        <Spinner size={8} color="gray" class="self-center" />
      {/if}
    </div>
  </Label>

  <Heading tag="h1" class="mb-4">Output</Heading>
  <div class="shadow-md min-w-96 min-h-96 rounded-lg bg-gray-100">
    {#each output as out}
      <P class="mx-4 my-2" size="lg">
        {out}
      </P>
    {/each}
  </div>
</div>
