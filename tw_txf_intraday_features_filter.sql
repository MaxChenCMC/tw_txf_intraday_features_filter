IF EXISTS 
(SELECT * FROM sys.all_objects WHERE object_id = object_id(N'tblMax_Market') AND type IN ('U'))
    DROP TABLE tblMax_Market
GO

create table tblMax_Market(
TradeDate date,
inst_f_buy bigint,
inst_t_buy bigint,
inst_txf_net numeric,
inst_txf_oi numeric,
inst_mtx_net numeric,
inst_mtx_oi numeric,
txf_open numeric,
txf_high numeric,
txf_low numeric,
txf_close numeric,
txf_volume numeric,
tse_open decimal(10,2),
tse_high decimal(10,2),
tse_low decimal(10,2),
tse_close decimal(10,2),
tse_volume decimal(10,2),
PutCallVol float,
PutCallRatio float
);
go

bulk insert tblMax_Market from '★data file path'
with (firstrow = 2, fieldterminator = ',', rowterminator = '\n');
go


drop table if exists #temp1;

with temp as (
	select TradeDate,
	case when (inst_t_buy = max(inst_t_buy) over (ORDER BY TradeDate rows BETWEEN 1 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature1',
	case when (avg(txf_close - txf_low) over (ORDER BY TradeDate rows BETWEEN 3 PRECEDING AND CURRENT ROW)
		  	  <avg(txf_high - txf_close) over (ORDER BY TradeDate rows BETWEEN 3 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature2',
	case when (2.7 * avg(abs(txf_open - txf_close)) over (ORDER BY TradeDate rows BETWEEN 4 PRECEDING AND CURRENT ROW)
			  <=     avg(txf_high - txf_low) over (ORDER BY TradeDate rows BETWEEN 4 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature3',
	case when (inst_f_buy = max(inst_f_buy) over (ORDER BY TradeDate rows BETWEEN 6 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature4',
	case when (PutcallRatio = max(PutcallRatio) over (ORDER BY TradeDate rows BETWEEN 8 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature5',
	case when (inst_txf_oi = max(inst_txf_oi) over (ORDER BY TradeDate rows BETWEEN 3 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature6',
	case when (PutCallVol = min(PutCallVol) over (ORDER BY TradeDate rows BETWEEN 4 PRECEDING AND CURRENT ROW)
	) then 1 else 0 end as 'feature7',
	txf_open, txf_high, txf_low, txf_close
	from tblMax_Market ) 

select 
	sum(cast(feature1 as int) + cast(feature2 as int) + cast(feature3 as int) + cast(feature4 as int) + cast(feature5 as int) + cast(feature6 as int) + cast(feature7 as int)) 
	over (ORDER BY TradeDate rows CURRENT ROW) as 'features',
	* into #temp1
FROM temp 

declare @stp int = 65;
declare @fee int = 4;
select *, 
case when lag(features) over (order by tradedate) >= 3
	 then ( case when txf_open - txf_low >= @stp then -(@stp + @fee)
			else txf_close - txf_open end)
	 else null end as 'pnl'
from #temp1
