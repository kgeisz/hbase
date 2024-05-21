Automated Triggers
==============
Trigger tests:
- [x] Smoke Tests
## Gatekeeping Tests [How to use ?](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/9945481342/Automated+gatekeeping#How-to-use-it.2)
(Select only one option from the list below) 
- [ ] Gate Smoke
- [ ] Fresh Install
#### Additional flags (Optional) Please add additional flags in the line below -C -O etc..



------------------------------------------
To override the job votes use the labels below (Only Gatekeepers and Admins) 

* CDH-BUILD                       = ci-build-pass
* SMOKE-TESTS                     = smoketest-pass
* GATEKEEPING-TICKET-VALIDATION   = gk-ticket-validation-pass
* GATEKEEPING-SMOKE-TESTS         = gk-system-test-pass
-----------
* To re-run build use Re-run all jobs / Re-run failed jobs option from the Github UI
* To re-run tests follow [label-based triggering](https://cloudera.atlassian.net/wiki/spaces/ENG/pages/10776412161)
