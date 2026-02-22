import asyncio
import unittest
from unittest.mock import patch, AsyncMock
import json
from datetime import datetime, timedelta

# Mocking openAI before import
with patch('openai.AsyncOpenAI'):
    from automation_engine import evaluate_single_condition, evaluate_condition_group, execute_workflow

class TestAdvancedConditioning(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.context = {
            "name": "John Doe",
            "balance": "$1,200.50",
            "status": "VIP",
            "tags": ["urgent", "callback"],
            "is_returning": "true",
            "last_seen": (datetime.now() - timedelta(days=5)).isoformat(),
            "empty_val": "",
            "null_val": None
        }

    def test_string_operators(self):
        self.assertTrue(evaluate_single_condition("Hello World", "starts_with", "Hello"))
        self.assertTrue(evaluate_single_condition("Hello World", "ends_with", "World"))
        self.assertTrue(evaluate_single_condition("user@example.com", "regex", r".+@.+\..+"))
        self.assertTrue(evaluate_single_condition("", "is_empty", ""))
        self.assertTrue(evaluate_single_condition("content", "is_not_empty", ""))
        self.assertFalse(evaluate_single_condition("content", "is_empty", ""))

    def test_number_operators(self):
        self.assertTrue(evaluate_single_condition("$1,200.50", ">", "1000"))
        self.assertTrue(evaluate_single_condition("42", "num_==", "42"))
        self.assertTrue(evaluate_single_condition("50", "<=", "50"))
        self.assertFalse(evaluate_single_condition("10", ">", "20"))

    def test_boolean_operators(self):
        self.assertTrue(evaluate_single_condition("true", "is_true", ""))
        self.assertTrue(evaluate_single_condition("yes", "is_true", ""))
        self.assertTrue(evaluate_single_condition("no", "is_false", ""))
        self.assertFalse(evaluate_single_condition("false", "is_true", ""))

    def test_date_operators(self):
        past = (datetime.now() - timedelta(days=10)).isoformat()
        future = (datetime.now() + timedelta(days=10)).isoformat()
        now = datetime.now().isoformat()
        
        self.assertTrue(evaluate_single_condition(now, "after", past))
        self.assertTrue(evaluate_single_condition(now, "before", future))

    def test_array_operators(self):
        self.assertTrue(evaluate_single_condition(["a", "b", "c"], "arr_contains", "b"))
        self.assertTrue(evaluate_single_condition('["x", "y"]', "arr_contains", "x")) # JSON string parsing
        self.assertTrue(evaluate_single_condition(["a", "b"], "length_>", "1"))
        self.assertFalse(evaluate_single_condition(["a"], "length_>", "5"))

    def test_complex_logic_group(self):
        # (Balance > 1000 AND Status == VIP) OR (Tags contains urgent)
        group = {
            "logic": "OR",
            "conditions": [
                {
                    "logic": "AND",
                    "conditions": [
                        {"field": "balance", "operator": ">", "value": "1000"},
                        {"field": "status", "operator": "==", "value": "VIP"}
                    ]
                },
                {"field": "tags", "operator": "arr_contains", "value": "urgent"}
            ]
        }
        self.assertTrue(evaluate_condition_group(group, self.context))
        
        # Test failure case
        fail_context = {"balance": "500", "status": "Standard", "tags": ["low_priority"]}
        self.assertFalse(evaluate_condition_group(group, fail_context))

    @patch('automation_engine.get_google_access_token', return_value="fake_token")
    @patch('automation_engine.update_google_sheet_row', new_callable=AsyncMock)
    @patch('automation_engine.find_row_index_for_update', return_value=5)
    async def test_workflow_routing_with_groups(self, mock_find, mock_update, mock_token):
        rule = {
            "steps": [
                {
                    "type": "sheets",
                    "operation_category": "write",
                    "update_mode": "update",
                    "sheet_id": "sheet_1",
                    "tab_name": "Default",
                    "routing_rules": [
                        {
                            "logic": "AND",
                            "conditions": [
                                {"field": "status", "operator": "==", "value": "VIP"},
                                {"field": "balance", "operator": ">", "value": "1000"}
                            ],
                            "target_tab": "Premium_Handling"
                        }
                    ],
                    "column_mapping": {"Status": "Upgrade needed"}
                }
            ]
        }
        
        # We need to simulate the workbook context being populated
        # In a real run, a previous 'read' step would do this.
        # For this test, we'll manually inject it into the execute_workflow flow or mock more.
        # Let's mock extract_single_value_with_llm to simulate context/extracted data.
        
        automation_data = {
            'google_tokens': {},
            'business_name': 'Test Biz',
            'system_prompt': 'Test Prompt',
            'rules': []
        }
        
        with patch('automation_engine.client.chat.completions.create') as mock_llm:
            mock_resp = AsyncMock()
            # Simulate LLM extracting data that matches our condition
            mock_resp.choices = [AsyncMock(message=AsyncMock(content='{"status": "VIP", "balance": "1500"}'))]
            mock_llm.return_value = mock_resp
            
            await execute_workflow("John is a VIP with 1500 balance", rule, automation_data, phone_number="123")
            
            # Check if it routed to Premium_Handling
            args, kwargs = mock_update.call_args
            self.assertEqual(kwargs['tab_name'], "Premium_Handling")
            print("\u2705 Workflow Group Routing passed!")

if __name__ == "__main__":
    unittest.main()
