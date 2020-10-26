USE [Temp]

select a.NE
	  ,case
		when left(NE,3) = 'IC1' then 'INNER CORE'
		else 'OUTER CORE'
	  end as NetworkRole
	  ,replace(a.[Interface], '(100M)', '') as Interface
	  ,case
	    when charindex('.', a.Interface) > 0 then 'Sub-Interfaz'
		else 'Interfaz'
	  end as Clase
	  ,case
	    when left(a.Interface,2) = 'hu' or left(a.Interface,2) = 'te' or left(a.Interface,2) = 'ge' then left(a.Interface,2)
		when left(a.Interface,5) = '100GE' then left(a.Interface,5)
		else left(a.Interface,2)
	  end as Tipo
      ,a.[Status]
      ,a.[Protocol]
      ,a.[Description]
from CU1_InterfacesAnsible as a
--left join Inventario.dbo.NI_Equipos as b on a.NE=b.Nombre
--where b.Modelo <> 'ATN910i'
