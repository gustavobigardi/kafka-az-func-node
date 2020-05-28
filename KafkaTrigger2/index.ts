import { AzureFunction, Context } from "@azure/functions"
import * as azureStorage from 'azure-storage';

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

    if (acao.Valor > 1000) {
        const service = azureStorage.createQueueService();
        service.createMessage('acao-alta', `[TESTE Azure Queue] -  Uma ação com valor de alto risco foi recebida com código [${acao.Codigo}] e valor [${acao.Valor}]`, (error, results, response) => {
        });
    }
};

export default kafkaTrigger;
