<?php

declare(strict_types=1);

namespace Xtreamwayz\Expressive\Messenger\Transport;

use Interop\Queue\Context;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\Serializer;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Xtreamwayz\Expressive\Messenger\Exception\RejectMessageException;
use Xtreamwayz\Expressive\Messenger\Exception\RequeueMessageException;

class EnqueueTransport implements TransportInterface
{
    /** @var Serializer */
    private $serializer;

    /** @var Context */
    private $psrContext;

    /** @var string */
    private $queueName;

    /** @var int */
    private $receiveTimeout;

    /** @var bool */
    private $shouldStop;

    public function __construct(Serializer $serializer, Context $psrContext, string $queueName)
    {
        $this->serializer     = $serializer;
        $this->psrContext     = $psrContext;
        $this->queueName      = $queueName;
        $this->receiveTimeout = 1000; // 1s
    }

    /**
     * Receive some messages to the given handler.
     *
     * The handler will have, as argument, the received {@link \Symfony\Component\Messenger\Envelope} containing the
     * message. Note that this envelope can be `null` if the timeout to receive something has expired.
     */
    public function receive(callable $handler) : void
    {
        $queue    = $this->psrContext->createQueue($this->queueName);
        $consumer = $this->psrContext->createConsumer($queue);

        while (! $this->shouldStop) {
            $message = $consumer->receive($this->receiveTimeout);
            if ($message === null) {
                continue;
            }

            try {
                $handler($this->serializer->decode([
                    'body'       => $message->getBody(),
                    'headers'    => $message->getHeaders(),
                    'properties' => $message->getProperties(),
                ]));

                $consumer->acknowledge($message);
            } catch (RejectMessageException $e) {
                $consumer->reject($message);
            } catch (RequeueMessageException $e) {
                $consumer->reject($message, true);
            } catch (\Throwable $e) {
                $consumer->reject($message);
            }
        }
    }

    /**
     * Sends the given envelope.
     */
    public function send(Envelope $envelope) : Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);
        $psrMessage     = $this->psrContext->createMessage(
            $encodedMessage['body'],
            $encodedMessage['properties'] ?? [],
            $encodedMessage['headers'] ?? []
        );

        $queue    = $this->psrContext->createQueue($this->queueName);
        $producer = $this->psrContext->createProducer();
        $producer->send($queue, $psrMessage);

        return $envelope;
    }

    /**
     * Stop receiving messages.
     */
    public function stop() : void
    {
        $this->shouldStop = true;
    }

    /**
     * Acknowledge that the passed message was handled.
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function ack(Envelope $envelope): void
    {
        // TODO: Implement ack() method.
    }

    /**
     * Called when handling the message failed and it should not be retried.
     *
     * @throws TransportException If there is an issue communicating with the transport
     */
    public function reject(Envelope $envelope): void
    {
        // TODO: Implement reject() method.
    }
}
