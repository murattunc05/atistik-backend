import unittest
from pathlib import Path


WORKFLOW_PATH = Path(__file__).parent / ".github" / "workflows" / "atistik-daily-automation.yml"


class WorkflowConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_manual_evaluate_gate_mode_and_strict_input_are_available(self):
        self.assertIn("- evaluate-gate", self.workflow)
        self.assertIn("strict_gate:", self.workflow)
        self.assertIn("type: boolean", self.workflow)
        self.assertIn("Fail evaluation gate on WARN/REVIEW as well as FAIL.", self.workflow)

    def test_evaluation_gate_dependencies_and_steps_are_wired(self):
        self.assertIn("name: Install evaluation dependencies", self.workflow)
        self.assertIn(
            "if: steps.resolve.outputs.mode == 'results' || steps.resolve.outputs.mode == 'evaluate-gate'",
            self.workflow,
        )
        self.assertIn("name: Run v4.18 evaluation gate", self.workflow)
        self.assertIn("name: Fail on evaluation gate failure", self.workflow)

    def test_evaluate_gate_reports_are_committed_before_manual_failure(self):
        self.assertIn('if [[ "${{ steps.resolve.outputs.mode }}" == "evaluate-gate" ]]; then', self.workflow)
        self.assertIn('echo "EVALUATION_GATE_EXIT=$automation_status" >> "$GITHUB_ENV"', self.workflow)
        self.assertIn("exit 0", self.workflow)
        self.assertIn("env.EVALUATION_GATE_EXIT != '' && env.EVALUATION_GATE_EXIT != '0'", self.workflow)
        commit_index = self.workflow.index("name: Commit automation reports")
        fail_index = self.workflow.index("name: Fail on evaluation gate failure")
        self.assertLess(commit_index, fail_index)

    def test_strict_gate_flag_is_passed_to_manual_and_results_gate_runs(self):
        self.assertGreaterEqual(self.workflow.count("STRICT_GATE_ARGS+=(--strict-gate)"), 2)
        self.assertGreaterEqual(self.workflow.count('"${STRICT_GATE_ARGS[@]}"'), 2)
        self.assertIn('STRICT_GATE="${{ inputs.strict_gate }}"', self.workflow)
        self.assertIn('echo "strict_gate=$STRICT_GATE" >> "$GITHUB_OUTPUT"', self.workflow)


if __name__ == "__main__":
    unittest.main()
