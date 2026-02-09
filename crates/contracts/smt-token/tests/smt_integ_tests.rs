use client_sdk::tests::assert_handle;
use hyli_smt_token::{FAUCET_ID, SmtTokenAction, account::AccountSMT};
use sdk::{BlobIndex, BlobTransaction, ContractAction};

#[test]
fn e2e_test_transfer() {
    let mut contract1 = AccountSMT::default();

    let tx = BlobTransaction::new(
        sdk::Identity("identityTest".into()),
        vec![
            SmtTokenAction::Transfer {
                sender: FAUCET_ID.into(),
                recipient: "recipient".into(),
                amount: 100,
            }
            .as_blob("testContract".into(), None, None),
        ],
    );

    assert_handle(&mut contract1, &tx, BlobIndex(0));

    let account = contract1.get_account(&"recipient".into()).unwrap();
    assert!(account.is_some(), "Recipient account should exist");
    let account = account.unwrap();

    assert_eq!(account.balance, 100, "Recipient balance should be 100");
}
