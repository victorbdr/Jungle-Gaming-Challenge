import 'reflect-metadata';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { type DynamicModule, type INestApplication, Module } from '@nestjs/common';
import { APP_FILTER, APP_GUARD, NestFactory } from '@nestjs/core';
import { LOGGER } from '../../application/ports';
import type { Container } from '../../bootstrap/container';
import type { SqsConfig } from '../sqs/sqs-config';
import { AuthGuard } from './auth';
import { CONTAINER, HealthController, SQS_CLIENT, SQS_CONFIG, WageringController, WalletsController } from './controllers';
import { ApplicationErrorFilter } from './error.filter';

@Module({})
export class HttpModule {
  static forRoot(container: Container, sqs: SQSClient, sqsConfig: SqsConfig): DynamicModule {
    return {
      module: HttpModule,
      controllers: [WalletsController, WageringController, HealthController],
      providers: [
        { provide: CONTAINER, useValue: container },
        { provide: LOGGER, useValue: container.logger },
        { provide: SQS_CLIENT, useValue: sqs },
        { provide: SQS_CONFIG, useValue: sqsConfig },
        { provide: APP_FILTER, useClass: ApplicationErrorFilter },
        { provide: APP_GUARD, useClass: AuthGuard },
      ],
    };
  }
}

export const createHttpApp = async (container: Container, sqs: SQSClient, sqsConfig: SqsConfig): Promise<INestApplication> => {
  const app = await NestFactory.create(HttpModule.forRoot(container, sqs, sqsConfig), { logger: ['error', 'warn'], bodyParser: true });
  app.enableShutdownHooks();
  return app;
};
