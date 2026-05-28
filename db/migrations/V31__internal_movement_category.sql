-- Consolidate legacy movement category names under the current product language.

INSERT INTO categories (user_id, name, kind, icon, is_archived)
SELECT DISTINCT user_id, 'Internal Movement', 'transfer', '⇄', false
FROM categories
WHERE name IN ('Transfer', 'Switching')
ON CONFLICT (user_id, name)
DO UPDATE SET kind='transfer', icon='⇄', is_archived=false;

UPDATE transactions t
SET category_id = current.category_id
FROM categories legacy
JOIN categories current
  ON current.user_id = legacy.user_id
 AND current.name = 'Internal Movement'
WHERE t.category_id = legacy.category_id
  AND legacy.name IN ('Transfer', 'Switching');

UPDATE categories
SET is_archived = true
WHERE name IN ('Transfer', 'Switching');
