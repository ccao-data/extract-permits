# extract-permits

Scripts and workflows for permit data extraction.

Currently, the only permits we extract are from the City of Chicago data
portal. The code that extracts these permits is defined in the [`chicago/`
subdirectory](./chicago/) and forms the basis of the
[`extract-chicago-permits`
workflow](https://github.com/ccao-data/extract-permits/actions/workflows/extract-chicago-permits.yaml).

## Running the workflow

To run the workflow, navigate to [the page for the
workflow](https://github.com/ccao-data/extract-permits/actions/workflows/extract-chicago-permits.yaml),
click the **Run workflow** button to open a dropdown, and select your parameters:

- **Use workflow from**: The git branch to use as the basis for running the
  workflow. Unless you are testing changes, this should always be `main`.
- **Start date**: The lower bound (inclusive) for a date range to use to filter
  permits. Must be in `YYYY-MM-DD` format.
- **End date**: The upper bound (inclusive) for a date range to use to filter
  permits. Must be in `YYYY-MM-DD` format.
- **Deduplicate**: Filter out permits that have already been extracted to our
  data warehouse. We recommend leaving this option unchecked because we have
  not extensively tested the deduplication logic. Instead, we only query
  mutually-exclusive date ranges of permits to send to the Data Integrity team.

Once the workflow finishes running, it will upload a ZIP archive containing the
permits to an AWS S3 bucket. It will also send a message containing a link to
the bucket to an AWS SNS topic dedicated to the workflow. If you subscribe to
that AWS SNS topic, you will receive an email with this link when the workflow
has finished running. Alternatively, the workflow will also print a link to the
S3 bucket in its logs, so you can check the logs instead of subscribing to
the SNS topic.

## Development

Follow these instructions if you need to make changes to the permit extraction
scripts.

These instructions are for Ubuntu, which is the only platform we've tested.

### Installation

#### Requirements

* Python3 with `uv` installed (pre-installed on the CCAO server)
* [AWS CLI installed
  locally](https://github.com/ccao-data/wiki/blob/master/How-To/Connect-to-AWS-Resources.md)
  * You'll also need permissions for Athena, Glue, and S3
* [`aws-mfa` installed locally](https://github.com/ccao-data/wiki/blob/master/How-To/Setup-the-AWS-Command-Line-Interface-and-Multi-factor-Authentication.md)

#### Install Python dependencies

Run the following commands to install Python dependencies:

```bash
cd chicago
uv sync --frozen
```

### Run the script

To run the script, make sure you're in the `chicago/` subdirectory:

```bash
cd chicago
```

You must also authenticate with AWS using MFA if you haven't already today:

```bash
aws-mfa
```

Then, run the script:

```bash
uv run python3 permit_cleaning.py \
  # The first argument is the lower bound for the date range (inclusive)
  <YYYY-MM-DD> \
  # The second argument is the upper bound for the date range (inclusive)
  <YYYY-MM-DD> \
  # Boolean indicating whether to filter out permits that are already in
  # our warehouse. We recommend not deduplicating because the logic has
  # not been extensively tested
  False
```

You can also run the script using the `extract-chicago-permits` workflow. See
[Running the workflow](#running-the-workflow) for instructions on how to do
that.
