package registry

import (
	"reflect"
	"testing"

	"github.com/openshift/ci-tools/pkg/api"
	"k8s.io/apimachinery/pkg/util/diff"
)

func TestResolve(t *testing.T) {
	reference1 := "generic-unit-test"
	teardownRef := "teardown"
	fipsPreChain := "install-fips"
	nestedChains := "nested-chains"
	chainInstall := "install-chain"
	awsWorkflow := "ipi-aws"
	for _, testCase := range []struct {
		name        string
		config      api.MultiStageTestConfiguration
		stepMap     ReferenceMap
		chainMap    ChainMap
		workflowMap WorkflowMap
		expectedRes api.MultiStageTestConfigurationLiteral
		expectErr   bool
	}{{
		// This is a full config that should not change (other than struct) when passed to the Resolver
		name: "Full AWS IPI",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.TestStep{{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-install",
					From:     "installer",
					Commands: "openshift-cluster install",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
			Test: []api.TestStep{{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "e2e",
					From:     "my-image",
					Commands: "make custom-e2e",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
			Post: []api.TestStep{{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-teardown",
					From:     "installer",
					Commands: "openshift-cluster destroy",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.LiteralTestStep{{
				As:       "ipi-install",
				From:     "installer",
				Commands: "openshift-cluster install",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Test: []api.LiteralTestStep{{
				As:       "e2e",
				From:     "my-image",
				Commands: "make custom-e2e",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Post: []api.LiteralTestStep{{
				As:       "ipi-teardown",
				From:     "installer",
				Commands: "openshift-cluster destroy",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
		},
		expectErr: false,
	}, {
		name: "Test with reference",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Test: []api.TestStep{{
				Reference: &reference1,
			}},
		},
		stepMap: ReferenceMap{
			reference1: {
				As:       "generic-unit-test",
				From:     "my-image",
				Commands: "make test/unit",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{
			ClusterProfile: api.ClusterProfileAWS,
			Test: []api.LiteralTestStep{{
				As:       "generic-unit-test",
				From:     "my-image",
				Commands: "make test/unit",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
		},
		expectErr: false,
	}, {
		name: "Test with broken reference",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Test: []api.TestStep{{
				Reference: &reference1,
			}},
		},
		stepMap: ReferenceMap{
			"generic-unit-test-2": {
				As:       "generic-unit-test-2",
				From:     "my-image",
				Commands: "make test/unit",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{},
		expectErr:   true,
	}, {
		name: "Test with chain and reference",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.TestStep{{
				Chain: &fipsPreChain,
			}},
			Test: []api.TestStep{{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "e2e",
					From:     "my-image",
					Commands: "make custom-e2e",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
			Post: []api.TestStep{{
				Reference: &teardownRef,
			}},
		},
		chainMap: ChainMap{
			fipsPreChain: {{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-install",
					From:     "installer",
					Commands: "openshift-cluster install",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}, {
				LiteralTestStep: &api.LiteralTestStep{
					As:       "enable-fips",
					From:     "fips-enabler",
					Commands: "enable_fips",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		stepMap: ReferenceMap{
			teardownRef: {
				As:       "ipi-teardown",
				From:     "installer",
				Commands: "openshift-cluster destroy",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				}},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.LiteralTestStep{{
				As:       "ipi-install",
				From:     "installer",
				Commands: "openshift-cluster install",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}, {
				As:       "enable-fips",
				From:     "fips-enabler",
				Commands: "enable_fips",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Test: []api.LiteralTestStep{{
				As:       "e2e",
				From:     "my-image",
				Commands: "make custom-e2e",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Post: []api.LiteralTestStep{{
				As:       "ipi-teardown",
				From:     "installer",
				Commands: "openshift-cluster destroy",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
		},
		expectErr: false,
	}, {
		name: "Test with broken chain",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Test: []api.TestStep{{
				Reference: &fipsPreChain,
			}},
		},
		chainMap: ChainMap{
			"broken": {{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "generic-unit-test-2",
					From:     "my-image",
					Commands: "make test/unit",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{},
		expectErr:   true,
	}, {
		name: "Test with nested chains",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.TestStep{{
				Chain: &nestedChains,
			}},
		},
		chainMap: ChainMap{
			nestedChains: {{
				Chain: &chainInstall,
			}, {
				LiteralTestStep: &api.LiteralTestStep{
					As:       "enable-fips",
					From:     "fips-enabler",
					Commands: "enable_fips",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
			chainInstall: {{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-lease",
					From:     "installer",
					Commands: "lease",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}, {
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-setup",
					From:     "installer",
					Commands: "openshift-cluster install",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.LiteralTestStep{{
				As:       "ipi-lease",
				From:     "installer",
				Commands: "lease",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}, {
				As:       "ipi-setup",
				From:     "installer",
				Commands: "openshift-cluster install",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}, {
				As:       "enable-fips",
				From:     "fips-enabler",
				Commands: "enable_fips",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				}},
			},
		},
		expectErr: false,
	}, {
		name: "Test with duplicate names after unrolling chains",
		config: api.MultiStageTestConfiguration{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.TestStep{{
				Chain: &nestedChains,
			}},
		},
		chainMap: ChainMap{
			nestedChains: {{
				Chain: &chainInstall,
			}, {
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-setup",
					From:     "installer",
					Commands: "openshift-cluster install",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
			chainInstall: {{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-lease",
					From:     "installer",
					Commands: "lease",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}, {
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-setup",
					From:     "installer",
					Commands: "openshift-cluster install",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{},
		expectErr:   true,
	}, {
		name: "Full AWS Workflow",
		config: api.MultiStageTestConfiguration{
			Workflow: &awsWorkflow,
		},
		chainMap: ChainMap{
			fipsPreChain: {{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "ipi-install",
					From:     "installer",
					Commands: "openshift-cluster install",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}, {
				LiteralTestStep: &api.LiteralTestStep{
					As:       "enable-fips",
					From:     "fips-enabler",
					Commands: "enable_fips",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		stepMap: ReferenceMap{
			teardownRef: {
				As:       "ipi-teardown",
				From:     "installer",
				Commands: "openshift-cluster destroy",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				}},
		},
		workflowMap: WorkflowMap{
			awsWorkflow: {
				ClusterProfile: api.ClusterProfileAWS,
				Pre: []api.TestStep{{
					Chain: &fipsPreChain,
				}},
				Test: []api.TestStep{{
					LiteralTestStep: &api.LiteralTestStep{
						As:       "e2e",
						From:     "my-image",
						Commands: "make custom-e2e",
						Resources: api.ResourceRequirements{
							Requests: api.ResourceList{"cpu": "1000m"},
							Limits:   api.ResourceList{"memory": "2Gi"},
						}},
				}},
				Post: []api.TestStep{{
					Reference: &teardownRef,
				}},
			},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{
			ClusterProfile: api.ClusterProfileAWS,
			Pre: []api.LiteralTestStep{{
				As:       "ipi-install",
				From:     "installer",
				Commands: "openshift-cluster install",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				}}, {
				As:       "enable-fips",
				From:     "fips-enabler",
				Commands: "enable_fips",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				}},
			},
			Test: []api.LiteralTestStep{{
				As:       "e2e",
				From:     "my-image",
				Commands: "make custom-e2e",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Post: []api.LiteralTestStep{{
				As:       "ipi-teardown",
				From:     "installer",
				Commands: "openshift-cluster destroy",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
		},
		expectErr: false,
	}, {
		name: "Workflow with Test and ClusterProfile overridden",
		config: api.MultiStageTestConfiguration{
			Workflow:       &awsWorkflow,
			ClusterProfile: api.ClusterProfileAzure4,
			Test: []api.TestStep{{
				LiteralTestStep: &api.LiteralTestStep{
					As:       "custom-e2e",
					From:     "test-image",
					Commands: "make custom-e2e-2",
					Resources: api.ResourceRequirements{
						Requests: api.ResourceList{"cpu": "1000m"},
						Limits:   api.ResourceList{"memory": "2Gi"},
					}},
			}},
		},
		workflowMap: WorkflowMap{
			awsWorkflow: {
				ClusterProfile: api.ClusterProfileAWS,
				Pre: []api.TestStep{{
					LiteralTestStep: &api.LiteralTestStep{
						As:       "ipi-install",
						From:     "installer",
						Commands: "openshift-cluster install",
						Resources: api.ResourceRequirements{
							Requests: api.ResourceList{"cpu": "1000m"},
							Limits:   api.ResourceList{"memory": "2Gi"},
						}},
				}},
				Test: []api.TestStep{{
					LiteralTestStep: &api.LiteralTestStep{
						As:       "e2e",
						From:     "my-image",
						Commands: "make custom-e2e",
						Resources: api.ResourceRequirements{
							Requests: api.ResourceList{"cpu": "1000m"},
							Limits:   api.ResourceList{"memory": "2Gi"},
						}},
				}},
				Post: []api.TestStep{{
					LiteralTestStep: &api.LiteralTestStep{
						As:       "ipi-teardown",
						From:     "installer",
						Commands: "openshift-cluster destroy",
						Resources: api.ResourceRequirements{
							Requests: api.ResourceList{"cpu": "1000m"},
							Limits:   api.ResourceList{"memory": "2Gi"},
						}},
				}},
			},
		},
		expectedRes: api.MultiStageTestConfigurationLiteral{
			ClusterProfile: api.ClusterProfileAzure4,
			Pre: []api.LiteralTestStep{{
				As:       "ipi-install",
				From:     "installer",
				Commands: "openshift-cluster install",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Test: []api.LiteralTestStep{{
				As:       "custom-e2e",
				From:     "test-image",
				Commands: "make custom-e2e-2",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
			Post: []api.LiteralTestStep{{
				As:       "ipi-teardown",
				From:     "installer",
				Commands: "openshift-cluster destroy",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{"cpu": "1000m"},
					Limits:   api.ResourceList{"memory": "2Gi"},
				},
			}},
		},
		expectErr: false,
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			ret, err := NewResolver(testCase.stepMap, testCase.chainMap, testCase.workflowMap).Resolve(testCase.config)
			if !testCase.expectErr && err != nil {
				t.Errorf("%s: expected no error but got: %s", testCase.name, err)
			}
			if testCase.expectErr && err == nil {
				t.Errorf("%s: expected error but got none", testCase.name)
			}
			if !reflect.DeepEqual(ret, testCase.expectedRes) {
				t.Errorf("%s: got incorrect output: %s", testCase.name, diff.ObjectReflectDiff(ret, testCase.expectedRes))
			}
		})
	}
}
