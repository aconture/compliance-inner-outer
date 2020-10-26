create table inv_itf(ShelfName varchar (50), Hardware varchar (30), NetworkRole varchar (25), ShelfOperationalState varchar (10), UserLabel varchar (20), Bandwidth varchar (20), PortOperationalState varchar (20), Info1 varchar (500), Type varchar (20));

create table par_inv_itf(ShelfName varchar (50), Hardware varchar (30), NetworkRole varchar (25), ShelfOperationalState varchar (10), UserLabel varchar (20), Bandwidth varchar (20), PortOperationalState varchar (20), Info1 varchar (500), Type varchar (20), concat varchar (90) );

par_inv_itf v2:
desde ne:shelfname, interface, EstadoRed (portoperationalstate), DescRed (info1)
desde lisy:NetworkRole, Hardware, Bandwidth, EstadoLISy (PortOperationalState), DescLisy (Info1)
nuevo: concat, EvEstado, Tipo (tipo registro Anterior|nuevo)

create table par_inv_itf(NetworkRole varchar (25), ShelfName varchar (50), Hardware varchar (30), interface varchar (20), Bandwidth varchar (20), concat varchar (90), EstadoRed varchar (20), EstadoLisy varchar (20), DescRed varchar (500), DescLisy varchar (500), EvEstado varchar (20), Tipo varchar (10));


create table ne(shelfname varchar (50), interface varchar (20), portoperationalstate varchar (20), protocol varchar (20), info1 varchar (500), concat varchar (90));



