Automated Triggers
==============
Trigger tests:

- [x] Smoke Tests
- [x] CI Pre-commit Tier Tests
------------------------------------------
To override the job votes use the labels below (Only Gatekeepers and Admins)

* CDH-BUILD                       = ci-build-pass
* SMOKE-TESTS                     = smoketest-pass
* PRECOMMIT-TIER-TESTS            = precommit-tier-tests-pass
* GATEKEEPING-TICKET-VALIDATION   = gk-ticket-validation-pass
-----------
* To re-run build use Re-run all jobs / Re-run failed jobs option from the Github UI
* To re-run tests follow [label-based triggering](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/10776412161)