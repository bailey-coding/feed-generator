import { pipeline } from '@huggingface/transformers'

import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

// https://huggingface.co/Xenova/twitter-roberta-base-sentiment-latest
// https://huggingface.co/docs/hub/transformers-js
// https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest

let pipe
async function getPipe() {
  if (pipe) return pipe
  pipe = await pipeline(
    'text-classification',
    'Xenova/twitter-roberta-base-sentiment-latest',
  )
  return pipe
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    // for (const post of ops.posts.creates) {
    //   console.log(post.record.text)
    // }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only queer-related posts
        return (
          create.record.text.toLowerCase().includes('queer') ||
          create.record.text.toLowerCase().includes('trans')
        )
      })
      .filter((create) => {
        return create.record.langs?.indexOf('en') !== -1
      })
      .filter(async (create) => {
        const out = (await (await getPipe())(create.record.text))[0]
        const score = out.score
        const label = out.label
        if (label != 'positive') {
          console.log(`Ignored message, result=${label}`)
          return false
        }
        console.log(`message: Score is ${score} - ${label}
          post: ${create.record.text}\n`)
        return true
      })
      .map((create) => {
        // console.table(create)
        // console.log(create.record.text)
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
