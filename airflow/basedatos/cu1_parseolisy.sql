USE [Temp]

select NetworkRole, ShelfName, Hardware, replace(UserLabel, '(100M)', '') as UserLabel, Bandwidth
	,case
		when NetworkRole = 'INNER CORE' and Bandwidth = '100 Gb' then '100GE'
		when NetworkRole = 'INNER CORE' and Bandwidth = '10 Gb' then 'GE'
		when NetworkRole = 'OUTER CORE' and Bandwidth in ('100 Gb', '100GB') then 'Hu'
		when NetworkRole = 'OUTER CORE' and Bandwidth = '10 Gb' then 'Te'
		else '-' 
	end as TipoCon
	, ShelfName + case
		when NetworkRole = 'INNER CORE' and Bandwidth = '100 Gb' then '100GE'
		when NetworkRole = 'INNER CORE' and Bandwidth = '10 Gb' then 'GE'
		when NetworkRole = 'OUTER CORE' and Bandwidth in ('100 Gb', '100GB') then 'Hu'
		when NetworkRole = 'OUTER CORE' and Bandwidth = '10 Gb' then 'Te'
		else '-' 
	end + replace(UserLabel, '(100M)', '') as Port
	, ShelfOperationalState
	, PortOperationalState
	, Info1
	, Type

from CU1_InterfacesLISy

where NetworkRole in ('INNER CORE','OUTER CORE') 
--ShelfName = 'CEN1NA' and 'Hu' + UserLabel in ('Hu0/6/0/0','Hu0/6/0/1','Hu0/6/0/2','Hu0/6/0/3')
--shelfname in ('IC1.HOR1','IC1.SLO1','BEL1NA','BUA1NA','CEN1NA','COE1NA','CTE1NA','IRD1NA','MUN1NA','MUS1NA','RES1NA','ROC2.AVA1','ROC2.BEL1','ROC2.HOR1','ROC2.SLO1','RSC1NA') 




