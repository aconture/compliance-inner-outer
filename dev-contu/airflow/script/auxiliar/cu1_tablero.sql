USE [Temp]

select a.NetworkRole, a.NE, b.Hardware, a.Interface, b.Bandwidth
	, a.Status as EstadoRed--, a.Protocol
	, b.PortOperationalState as EstadoLISy
	, a.Description as DescRed, b.Info1 as DescLISy
	, case
		when b.ShelfName is null then 'Inexistente'
		when a.Status = 'up' and b.PortOperationalState = 'active' then 'Ok'
		when a.Status = 'up' and b.PortOperationalState <> 'active' then 'Revisar'
		when a.Status like '%down%' and b.PortOperationalState <> 'active' then 'Ok'
		when a.Status like '%down%' and b.PortOperationalState = 'active' then 'Revisar'
		else 'Otro'
	end as EvEstado
	, case
		when b.ShelfName is null then '5'
		when a.Status = 'up' and b.PortOperationalState = 'active' then null
		when a.Status = 'up' and b.PortOperationalState <> 'active' then '1/2'
		when a.Status like '%down%' and b.PortOperationalState <> 'active' then null
		when a.Status like '%down%' and b.PortOperationalState = 'active' then '3/4'
		else '0'
	end as CodError


from CU1_ParseoAnsible as a
left join CU1_ParseoLISyIP as b on a.NE + a.Interface = b.Port

where a.Tipo in ('hu','te','100ge','GE') and a.Clase = 'interfaz' and a.ne + a.interface not in ('IC1.HOR1GE0/0/0','IC1.SLO1GE0/0/0')



