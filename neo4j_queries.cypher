// Multi-hop co-purchase query
// Finds products bought by customers who share purchase history with a target customer
MATCH (target:Customer {customer_id: $customerId})-[:PURCHASED]->(:Product)<-[:PURCHASED]-(other:Customer)-[:PURCHASED]->(rec:Product)
WHERE NOT (target)-[:PURCHASED]->(rec)
RETURN rec.product_id, count(*) AS strength
ORDER BY strength DESC
LIMIT 10;

// Seller network query
// Identifies high-degree sellers through shared product categories
MATCH (s1:Seller)-[:SOLD]->(p1:Product)<-[:SOLD]-(s2:Seller)
WHERE s1 <> s2
RETURN s1.seller_id, s2.seller_id, count(DISTINCT p1) AS shared
ORDER BY shared DESC
LIMIT 10;

// Customer similarity query
// Finds customers with overlapping purchase histories within two hops
MATCH (c1:Customer {customer_id: $customerId})-[:PURCHASED]->(:Product)<-[:PURCHASED]-(c2:Customer)
WHERE c1 <> c2
RETURN c2.customer_id, count(*) AS overlap
ORDER BY overlap DESC
LIMIT 10;