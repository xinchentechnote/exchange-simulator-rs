pub mod base_info_service;
pub mod config;

use crate::base_info_service::{load_account_info, load_symbol_info};
use matching_core::core::exchange::{ExchangeConfig, ExchangeCore, ExchangeState};
use matching_core::{
    CoreSymbolSpecification, OrderAction, OrderCommand, OrderCommandType, OrderType, SymbolType,
};
use std::sync::Arc;

fn main() {
    let config = ExchangeConfig::default();
    let mut core = ExchangeCore::new(config);

    core.set_result_consumer(Arc::new(|cmd| {
        println!("result: {:?} - {:?}", cmd.command, cmd.result_code);
    }));

    // 添加标的
    load_symbol_info("data/sse/symbol_data.csv")
        .into_iter()
        .for_each(|spec| core.add_symbol(spec));
    let user_balances = load_account_info("data/sse/account_data.csv");
    // 添加用户
    user_balances.iter().for_each(|user_balance| {
        core.submit_command(OrderCommand {
            command: OrderCommandType::AddUser,
            uid: user_balance.uid,
            ..Default::default()
        });
    });

    // 充值
    user_balances.iter().for_each(|user_balance| {
        core.submit_command(OrderCommand {
            command: OrderCommandType::BalanceAdjustment,
            uid: user_balance.uid,
            symbol: user_balance.balance,
            price: user_balance.amount,
            ..Default::default()
        });
    });
}
