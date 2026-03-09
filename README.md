# Fog Car Insurance Real-Time Pricing

This repository contains a finishable Fog and Edge Computing coursework project built with Python and AWS.

For a full Chinese architecture walkthrough, see `ARCHITECTURE_CN.md`.

## Architecture

- `edge/`: interpolation-based sensor simulator
- `fog/`: local fog node that aggregates sensor events and publishes enriched telemetry
- `cloud/lambda_ingest/`: Lambda that consumes SQS and writes to Amazon Timestream
- `cloud/dashboard/`: Dash dashboard for visualising risk and premium metrics
- `infra/template.yaml`: SAM/CloudFormation template for AWS deployment

## Local prerequisites

- Python 3.11+
- Docker and Docker Compose
- AWS CLI
- AWS SAM CLI

## Quick start

1. Use the bundled sample dataset or place the Kaggle CSV in `data/`.
2. Review `edge/config.yaml` and `fog/config.yaml`.
3. Run the local pipeline:

   ```bash
   docker compose up --build
   ```

By default, the fog node publishes aggregated payloads to the console. Switch to AWS IoT Core by setting:

- `MQTT_MODE=aws_iot`
- `AWS_IOT_ENDPOINT`
- `AWS_IOT_CA_PATH`
- `AWS_IOT_CERT_PATH`
- `AWS_IOT_KEY_PATH`

## Running tests

```bash
python -m pip install -r requirements-dev.txt
pytest
ruff check .
```

## Deploying with SAM and GitHub Actions

1. Create a GitHub repository and push this project.
2. Add GitHub Actions secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_REGION`
3. The deploy workflow will:
   - run `sam build`
   - deploy SQS, Lambda, Timestream, IoT rule, ECR, and IAM roles
   - build and push the dashboard image to ECR
   - redeploy SAM with the dashboard image URI so App Runner is created

## AWS bootstrap notes

- You still need a one-time AWS IoT Core thing, certificate, and policy for the fog node.
- The IoT Topic Rule is created by `infra/template.yaml`.
- The dashboard uses the App Runner instance role created by SAM to query Timestream.

## Demo mode

The repository also includes an optional Lambda-based demo mode for the dashboard.

- `Start Demo` invokes a Lambda control function
- Step Functions repeatedly triggers a demo generator Lambda
- The generator sends demo telemetry to SQS
- The existing ingest Lambda writes demo records into Timestream
- The dashboard switches to the demo session view only when a user explicitly starts a demo

This means the coursework path is preserved:

- local `edge -> fog -> AWS IoT Core -> SQS -> ingest Lambda -> Timestream`
- demo mode is a separate cloud-only path
- demo data is marked with `mode=demo` and `demo_session_id`
- coursework data remains `mode=production`

## Suggested GitHub flow

- Work on feature branches
- Open pull requests to trigger `.github/workflows/ci.yml`
- Merge to `main` to trigger `.github/workflows/deploy.yml`

## Coursework deliverables covered by this repo

- Sensor & fog application
- Scalable backend application
- Dashboard frontend
- Docker packaging
- SAM deployment
- GitHub Actions CI/CD
