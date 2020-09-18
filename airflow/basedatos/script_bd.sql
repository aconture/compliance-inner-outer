#ShelfName|Hardware|NetworkRole|ShelfOperationalState|UserLabel|Bandwidth|PortOperationalState|Info1|Type

#shelfName|shelfHardware|shelfNetworkRole|shelfOperationalState|portInterfaceName|portBandwidth|portOperationalState|portInfo1|portType 

DROP TABLE par_inv_itf;

DROP TABLE inv_itf;

CREATE TABLE public.par_inv_itf (
    shelfname character varying(50),
    shelfHardware character varying(30),
    networkrole character varying(25),
    shelfoperationalstate character varying(10),
    userlabel character varying(20),
    bandwidth character varying(20),
    portoperationalstate character varying(20),
    info1 character varying(500),
    type character varying(20),
    concat character varying(90)
);


ALTER TABLE public.par_inv_itf OWNER TO airflow;

CREATE TABLE public.inv_itf (
    shelfname character varying(50),
    shelfHardware character varying(30),
    networkrole character varying(25),
    shelfoperationalstate character varying(10),
    userlabel character varying(20),
    bandwidth character varying(20),
    portoperationalstate character varying(20),
    info1 character varying(500),
    type character varying(20)
);


ALTER TABLE public.inv_itf OWNER TO airflow;

CREATE TABLE public.ne (
    shelfname character varying(50),
    interface character varying(20),
    portoperationalstate character varying(20),
    protocol character varying(20),
    info1 character varying(500),
    concat character varying(90)
);


ALTER TABLE public.ne OWNER TO airflow;
