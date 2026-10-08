-- acc_demo_1 is seeded generously (not just enough for one demo transfer) because
-- Task 24's end-to-end test and every manual README walkthrough draw from it
-- repeatedly over the life of this project, and confirmed_balance only ever
-- decreases for it (nothing credits it back).
INSERT INTO balance_projection (account_id, confirmed_balance) VALUES
  ('acc_demo_1', 100000),
  ('acc_demo_2', 5000),
  ('acc_psp_fail_demo', 5000)
ON CONFLICT (account_id) DO NOTHING;
