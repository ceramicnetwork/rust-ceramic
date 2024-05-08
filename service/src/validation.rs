// async fn insert_items_int(&self, items: &[ReconItem<'_, EventId>]) -> Result<InsertResult> {
//     if items.is_empty() {
//         return Ok(InsertResult::new(vec![], 0));
//     }
//     let mut to_add = vec![];
//     for item in items {
//         if let Some(val) = item.value {
//             match EventRaw::try_build(item.key.to_owned(), val).await {
//                 Ok(parsed) => to_add.push(parsed),
//                 Err(error) => {
//                     tracing::warn!(%error, order_key=%item.key, "Error parsing event into carfile");
//                     continue;
//                 }
//             }
//         } else {
//             to_add.push(EventRaw::new(item.key.clone(), vec![]));
//         }
//     }
//     if to_add.is_empty() {
//         return Ok(InsertResult::new(vec![], 0));
//     }
//     let mut new_keys = vec![false; to_add.len()];
//     let mut new_val_cnt = 0;
//     let mut tx = self.pool.writer().begin().await.map_err(Error::from)?;

//     for (idx, item) in to_add.into_iter().enumerate() {
//         let new_key = self.insert_key_int(&item.order_key, &mut tx).await?;
//         for block in item.blocks.iter() {
//             self.insert_event_block_int(block, &mut tx).await?;
//             self.mark_ready_to_deliver(&item.order_key, &mut tx).await?;
//         }
//         new_keys[idx] = new_key;
//         if !item.blocks.is_empty() {
//             new_val_cnt += 1;
//         }
//     }
//     tx.commit().await.map_err(Error::from)?;
//     let res = InsertResult::new(new_keys, new_val_cnt);

//     Ok(res)
// }
