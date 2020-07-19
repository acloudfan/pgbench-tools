SELECT
  tps.server,tps.set,tps.script,tps.scale,tps.clients,tps.workers,
  tps.tps as tps,
  lat.set,lat.script,lat.scale,lat.clients,lat.workers,
  lat.p90_latency 
FROM 
(
	SELECT
	  server,set,script,scale,clients,workers,
	  round(tps) AS tps
	FROM
	(
	  SELECT
	    server,set,script,scale,clients,workers,
	    max(tps) AS tps--,
	    --percentile_90_latency
	  FROM tests
	  GROUP BY server,set,script,scale,clients,workers--,percentile_90_latency
	) as g1
) AS tps,
--ORDER BY tps DESC 
LATERAL (
	SELECT
	  server,set,script,scale,clients,workers,
	  --round(tps) as tps,
	  p90_latency
	FROM
	(
	 SELECT
	    server,set,script,scale,clients,workers,
	    --tps,
	    min(percentile_90_latency) AS p90_latency
	  FROM tests
	  WHERE
	        percentile_90_latency IS NOT NULL
	  GROUP BY server,set,script,scale,clients,workers--,tps
	) AS g2
)  lat 
WHERE 
	lat.set = tps.set
AND     lat.server=tps.server
AND     lat.scale=tps.scale
AND     lat.clients=tps.clients
AND     lat.workers=tps.workers
--GROUP BY tps.set,tps.script,tps.scale,tps.clients,tps.workers, lat.set,lat.script,lat.scale,lat.clients,lat.workers,lat.p90_latency
ORDER BY  lat.p90_latency, tps.tps DESC LIMIT 20
