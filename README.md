# tap-planfix

`tap-planfix` is a Singer tap for Planfix.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation


```bash
pipx install git+https://github.com/epoch8/tap-planfix@master
```

## Configuration

### Source Authentication and Authorization

#### Create .env file with 

#### PLANFIX_URL - your planfix url 

#### PLANFIX_TOKEN - your planfix token

## Usage

You can easily run `tap-planfix` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-planfix --version
tap-planfix --help
tap-planfix --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_planfix/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-planfix` CLI interface directly using `poetry run`:

```bash
poetry run tap-planfix --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-planfix
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-planfix --version
# OR run a test `elt` pipeline:
meltano elt tap-planfix target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
