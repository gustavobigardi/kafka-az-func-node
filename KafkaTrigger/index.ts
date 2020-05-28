import { AzureFunction, Context } from "@azure/functions"
import { MongoClient, ObjectID } from 'mongodb';
import * as assert from 'assert';

class KafkaEvent {
    Offset : number;
    Partition : number;
    Topic : string;
    Timestamp : string;
    Value : string;
    
    constructor(metadata:any) {
        this.Offset = metadata.Offset;
        this.Partition = metadata.Partition;
        this.Topic = metadata.Topic;
        this.Timestamp = metadata.Timestamp;
        this.Value = metadata.Value;
    }

    public getValue<T>() : T {
        return JSON.parse(this.Value);
    }
}

interface Acao {
    Codigo : string;
    Valor : number;
}

const kafkaTrigger: AzureFunction = async function (context: Context, myQueueItem: string): Promise<void> {
    let evento = new KafkaEvent(eval(myQueueItem));
    let acao : Acao = evento.getValue<Acao>();

    context.log(`Ação recebida com código [${acao.Codigo}] e valor [${acao.Valor}]`);

    const mongoUrl = process.env.demoazurekafka_COSMOSDB;
    
    MongoClient.connect(mongoUrl, (error, client) => {
        assert.equal(error, null);
        const db = client.db('demokafka');
        const acaoDoc = {
            codigo: acao.Codigo,
            valor: acao.Valor,
            tipo: acao.Valor > 1000 ? 'Risco Alto' : 'Risco Baixo'
        }

        db.collection('acoes').insertOne(acaoDoc, (error, result) => {
            assert.equal(error, null);
            context.log(`Ação inserida no CosmosDb (Mongo APU) com ID: [${result.insertedId}]`);
            client.close();
        })
    });
};

export default kafkaTrigger;
