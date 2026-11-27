# `publish_to_repository`: configuration for QA and production

QA and production run the same image. Everything that differs between them is configured in Airflow (two connections, one shared connection and one Variable), not in the code. This page lists what to set in each environment. How the DAG works and its params: see the `publish_to_repository` section of [`airflow/README.md`](../../../README.md).

## Summary

| What | QA | Production |
| --- | --- | --- |
| `repository_cern_conn` host | `https://sandbox-cds-rdm.web.cern.ch` | `https://repository.cern` |
| `repository_cern_conn` password | Token of the **sandbox** | Token of **production** |
| `atom_conn` host | `https://archives.cern.ch` | `https://archives.cern.ch` |
| `atom_conn` password | AtoM REST API key | AtoM REST API key |
| `aws_s3_cern` | CERN S3 keys + `endpoint_url` extra | CERN S3 keys + `endpoint_url` extra |
| `publish_to_repository` Variable | Test bucket, folder and community | Production bucket, folder and community |

> **There is no test AtoM.** QA and production both write to the real archives.cern.ch. In QA, run `Deposit and link` only on test descriptions (for example the `CERN-ARCH-Test` series), and use `Dry Run` or `Deposit only` for everything else.

## Connections

Set them in **Admin → Connections**, or as `AIRFLOW_CONN_<CONN_ID>` environment variables in the deployment (`cern-sis/kubernetes-shared`). Keep tokens and keys in the secrets of the deployment, never in this repository.

### `repository_cern_conn` (repository.cern / CDS RDM)

| Field | QA | Production |
| --- | --- | --- |
| Type | HTTP | HTTP |
| Host | `https://sandbox-cds-rdm.web.cern.ch` | `https://repository.cern` |
| Password | Personal token of the sandbox | Personal token of production |

- Create the token in the instance where it is used (**Applications → Personal access tokens**, scope `deposit:write`). A sandbox token does not work in production, and the other way round.
- Use a **service account**, not a personal account: the records are owned by the account of the token, and only the owner can update them later (`refresh_metadata`, `replace_file`).
- For `auto_accept`, the account must be a **curator** of the community. Otherwise, every new record waits for a curator, and a second run links it to AtoM.
- The host must start with `https://`.

```bash
AIRFLOW_CONN_REPOSITORY_CERN_CONN='{"conn_type": "http", "host": "https://sandbox-cds-rdm.web.cern.ch", "password": "<token>"}'
```

### `atom_conn` (CERN Archive AtoM)

| Field | QA | Production |
| --- | --- | --- |
| Type | HTTP | HTTP |
| Host | `https://archives.cern.ch` | `https://archives.cern.ch` |
| Password | AtoM REST API key | AtoM REST API key |

- The account of the key needs the **Editor** group (it reads drafts and adds digital objects).
- Use a dedicated account. Generate its key with a script in the `atom-web` pod rather than with **Admin → Users → Edit**: saving that form can set the account to inactive.
- In AtoM, **Admin → Settings → Visible elements → Digital object metadata** must be visible. Otherwise, the DAG cannot see that a description already has a digital object.

```bash
AIRFLOW_CONN_ATOM_CONN='{"conn_type": "http", "host": "https://archives.cern.ch", "password": "<api key>"}'
```

### `aws_s3_cern` (CERN S3)

Already used by the other DAGs. Check that:

- the extra has `{"endpoint_url": "https://s3.cern.ch"}`. Without it, boto3 calls Amazon AWS and fails with `InvalidAccessKeyId`;
- the keys (OpenStack EC2 credentials of the project that owns the bucket) can **read, write and delete** in the bucket: the DAG moves the linked files to `migrated/` (copy, then delete) and can upload its report.

```bash
AIRFLOW_CONN_AWS_S3_CERN='{"conn_type": "aws", "login": "<access key>", "password": "<secret key>", "extra": {"endpoint_url": "https://s3.cern.ch"}}'
```

## Variable `publish_to_repository`

A JSON object, in **Admin → Variables** or as `AIRFLOW_VAR_PUBLISH_TO_REPOSITORY`.

QA:

```json
{
  "bucket": "cern-archives-multilateralism",
  "prefix": "atom test",
  "extension": ".pdf,.jpg",
  "community": "multilateralism"
}
```

Production (bucket and folder to be decided; check the community before the first run):

```json
{
  "bucket": "<production bucket>",
  "prefix": "<folder with the files to publish>",
  "extension": ".pdf",
  "community": "cern-digitized-archives-special-collections"
}
```

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `bucket` | yes | | |
| `prefix` | yes | | Folder with the `<AtoM slug>.<extension>` files. |
| `community` | yes | | The slug of the community **on the host of `repository_cern_conn`**. The sandbox and production have different communities: `multilateralism` exists on the sandbox. |
| `extension` | no | `.pdf` | Comma-separated. |
| `resource_type` | no | `publication` | |
| `done_folder` | no | `migrated` | `""` keeps the files in place. |
| `reports_folder` | no | `reports` | Where `upload_report` writes `publish_report_<run_id>.json`. `""` writes it in `prefix`. |
| `max_deposits` | no | `50` | New records per run. |

A missing required key, an unknown key (a typo) or a wrong type fails the run before it reads S3. The trigger form shows the values of the Variable; after a change, they appear at the next parse of the DAG file (or run `airflow dags reserialize`).

## Deployment

| Environment | How it is deployed |
| --- | --- |
| QA | Merge into `main`: `.github/workflows/build.yml` builds the image and updates QA in `cern-sis/kubernetes-shared`. |
| Production | Publish a GitHub release. |

## First run in a new environment

1. Set the three connections and the Variable.
2. Unpause the DAG (new DAGs start paused).
3. Trigger a `Dry Run`. In the `list_s3_files` log, check the `Settings:` line. In the `report` log, check the statuses and links.
4. Trigger `Deposit only` with one slug in `slugs`. Check the record on repository.cern (metadata, file, community).
5. After the community accepts it (or with `auto_accept`), trigger `Deposit and link` with the same slug. Check the description in AtoM: the digital object has the file link and a thumbnail.
6. Run without `slugs` for the whole folder.

## Checklist

| | QA | Production |
| --- | :-: | :-: |
| `repository_cern_conn`: host + token of the same instance | ☐ | ☐ |
| Token account is a service account (and a curator, for `auto_accept`) | ☐ | ☐ |
| `atom_conn`: API key of an active Editor account | ☐ | ☐ |
| `aws_s3_cern`: `endpoint_url` extra, write and delete rights on the bucket | ☐ | ☐ |
| `publish_to_repository` Variable: bucket, prefix, community of this environment | ☐ | ☐ |
| AtoM: digital object metadata visible | ☐ | ☐ |
| DAG unpaused, `Dry Run` checked | ☐ | ☐ |
