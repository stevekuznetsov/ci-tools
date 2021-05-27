package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/common/model"
)

func TestInfoForMetric(t *testing.T) {
	var testCases = []struct {
		name       string
		metric     model.Metric
		conf, meta *identifierQueryMetadata
	}{
		{
			name: "step pod",
			metric: model.Metric{
				model.LabelName("label_ci_openshift_io_metadata_org"):     "org",
				model.LabelName("label_ci_openshift_io_metadata_repo"):    "repo",
				model.LabelName("label_ci_openshift_io_metadata_branch"):  "branch",
				model.LabelName("label_ci_openshift_io_metadata_variant"): "variant",
				model.LabelName("label_ci_openshift_io_metadata_target"):  "target",
				model.LabelName("label_ci_openshift_io_metadata_step"):    "step",
				model.LabelName("pod"):                                    "pod",
				model.LabelName("container"):                              "container",
			},
			conf: &identifierQueryMetadata{
				table: "conf",
				columns: []columnValue{
					{column: "org", value: "org"},
					{column: "repo", value: "repo"},
					{column: "branch", value: "branch"},
					{column: "variant", value: "variant"},
				},
			},
			meta: &identifierQueryMetadata{
				table: "steps",
				columns: []columnValue{
					{column: "conf_id", value: 1},
					{column: "target", value: "target"},
					{column: "step", value: "step"},
					{column: "container", value: "container"},
				},
			},
		},
		{
			name: "build pod",
			metric: model.Metric{
				model.LabelName("label_ci_openshift_io_metadata_org"):     "org",
				model.LabelName("label_ci_openshift_io_metadata_repo"):    "repo",
				model.LabelName("label_ci_openshift_io_metadata_branch"):  "branch",
				model.LabelName("label_ci_openshift_io_metadata_variant"): "variant",
				model.LabelName("label_ci_openshift_io_metadata_target"):  "target",
				model.LabelName("label_openshift_io_build_name"):          "src",
				model.LabelName("pod"):                                    "src-build",
				model.LabelName("container"):                              "container",
			},
			conf: &identifierQueryMetadata{
				table: "conf",
				columns: []columnValue{
					{column: "org", value: "org"},
					{column: "repo", value: "repo"},
					{column: "branch", value: "branch"},
					{column: "variant", value: "variant"},
				},
			},
			meta: &identifierQueryMetadata{
				table: "builds",
				columns: []columnValue{
					{column: "conf_id", value: 1},
					{column: "build", value: "src"},
					{column: "container", value: "container"},
				},
			},
		},
		{
			name: "release pod",
			metric: model.Metric{
				model.LabelName("label_ci_openshift_io_metadata_org"):     "org",
				model.LabelName("label_ci_openshift_io_metadata_repo"):    "repo",
				model.LabelName("label_ci_openshift_io_metadata_branch"):  "branch",
				model.LabelName("label_ci_openshift_io_metadata_variant"): "variant",
				model.LabelName("label_ci_openshift_io_metadata_target"):  "target",
				model.LabelName("label_ci_openshift_io_release"):          "latest",
				model.LabelName("pod"):                                    "release-latest-cli",
				model.LabelName("container"):                              "container",
			},
			conf: &identifierQueryMetadata{
				table: "conf",
				columns: []columnValue{
					{column: "org", value: "org"},
					{column: "repo", value: "repo"},
					{column: "branch", value: "branch"},
					{column: "variant", value: "variant"},
				},
			},
			meta: &identifierQueryMetadata{
				table: "releases",
				columns: []columnValue{
					{column: "conf_id", value: 1},
					{column: "pod", value: "release-latest-cli"},
					{column: "container", value: "container"},
				},
			},
		},
		{
			name: "RPM repo pod",
			metric: model.Metric{
				model.LabelName("label_ci_openshift_io_metadata_org"):     "org",
				model.LabelName("label_ci_openshift_io_metadata_repo"):    "repo",
				model.LabelName("label_ci_openshift_io_metadata_branch"):  "branch",
				model.LabelName("label_ci_openshift_io_metadata_variant"): "variant",
				model.LabelName("label_ci_openshift_io_metadata_target"):  "target",
				model.LabelName("label_app"):                              "rpm-repo",
				model.LabelName("pod"):                                    "rpm-repo-5d88d4fc4c-jg2xb",
				model.LabelName("container"):                              "rpm-repo",
			},
			conf: &identifierQueryMetadata{
				table: "conf",
				columns: []columnValue{
					{column: "org", value: "org"},
					{column: "repo", value: "repo"},
					{column: "branch", value: "branch"},
					{column: "variant", value: "variant"},
				},
			},
			meta: &identifierQueryMetadata{
				table: "rpms",
				columns: []columnValue{
					{column: "conf_id", value: 1},
				},
			},
		},
		{
			name: "raw prowjob pod",
			metric: model.Metric{
				model.LabelName("label_created_by_prow"):           "true",
				model.LabelName("label_prow_k8s_io_refs_org"):      "org",
				model.LabelName("label_prow_k8s_io_refs_repo"):     "repo",
				model.LabelName("label_prow_k8s_io_refs_base_ref"): "branch",
				model.LabelName("label_prow_k8s_io_context"):       "context",
				model.LabelName("pod"):                             "d316d4cc-a437-11eb-b35f-0a580a800e92",
				model.LabelName("container"):                       "container",
			},
			conf: &identifierQueryMetadata{
				table: "conf",
				columns: []columnValue{
					{column: "org", value: "org"},
					{column: "repo", value: "repo"},
					{column: "branch", value: "branch"},
					{column: "variant", value: ""},
				},
			},
			meta: &identifierQueryMetadata{
				table: "prowjobs",
				columns: []columnValue{
					{column: "conf_id", value: 1},
					{column: "context", value: "context"},
					{column: "container", value: "container"},
				},
			},
		},
		{
			name: "raw periodic prowjob pod without context",
			metric: model.Metric{
				model.LabelName("label_created_by_prow"):           "true",
				model.LabelName("label_prow_k8s_io_refs_org"):      "org",
				model.LabelName("label_prow_k8s_io_refs_repo"):     "repo",
				model.LabelName("label_prow_k8s_io_refs_base_ref"): "branch",
				model.LabelName("label_prow_k8s_io_context"):       "",
				model.LabelName("label_prow_k8s_io_job"):           "periodic-ci-org-repo-branch-context",
				model.LabelName("label_prow_k8s_io_type"):          "periodic",
				model.LabelName("pod"):                             "d316d4cc-a437-11eb-b35f-0a580a800e92",
				model.LabelName("container"):                       "container",
			},
			conf: &identifierQueryMetadata{
				table: "conf",
				columns: []columnValue{
					{column: "org", value: "org"},
					{column: "repo", value: "repo"},
					{column: "branch", value: "branch"},
					{column: "variant", value: ""},
				},
			},
			meta: &identifierQueryMetadata{
				table: "prowjobs",
				columns: []columnValue{
					{column: "conf_id", value: 1},
					{column: "context", value: "context"},
					{column: "container", value: "container"},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			conf, metaGen := infoForMetric(testCase.metric)
			meta := metaGen(1)
			if diff := cmp.Diff(testCase.conf, conf, cmp.AllowUnexported(identifierQueryMetadata{}, columnValue{})); diff != "" {
				t.Errorf("%s: got incorrect configuration identifier query metadata from metric: %v", testCase.name, diff)
			}
			if diff := cmp.Diff(testCase.meta, meta, cmp.AllowUnexported(identifierQueryMetadata{}, columnValue{})); diff != "" {
				t.Errorf("%s: got incorrect identifier query metadata from metric: %v", testCase.name, diff)
			}
		})
	}
}
