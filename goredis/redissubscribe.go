package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type Subscribe struct {
	// 不可修改
	ctx context.Context
	r   *Redis

	sub     *redis.PubSub
	channel string
}

// 创建一个订阅对象，并订阅成功
func (r *Redis) CreateSubscribe(ctx context.Context, channel string) (*Subscribe, error) {
	// 创建订阅者
	subscriber, err := r.subscribe(ctx, channel)
	if err != nil {
		return nil, err
	}
	return &Subscribe{
		ctx:     utils.CtxNolog(context.TODO()), // 不要日志
		r:       r,
		sub:     subscriber,
		channel: channel,
	}, nil
}

// 创建一个订阅对象，并订阅成功
func (r *Redis) subscribe(ctx context.Context, channel string) (*redis.PubSub, error) {
	// 创建订阅者
	subscriber := r.Subscribe(ctx)
	// 订阅频道
	err := subscriber.Subscribe(ctx, channel)
	if err != nil {
		subscriber.Close()
		log.Error().Err(err).Str("channel", channel).Msg("Redis Subscribe fail")
		return nil, err
	}
	msg, err := subscriber.ReceiveTimeout(ctx, time.Second*3)
	if err != nil {
		subscriber.Close()
		log.Error().Err(err).Str("channel", channel).Msg("Redis Subscribe Receive fail")
		return nil, err
	}
	switch msg := msg.(type) {
	case *redis.Subscription:
		return subscriber, nil // 理论上读取的都是这个
	case *redis.Pong:
		return subscriber, nil
	case *redis.Message:
		return subscriber, nil
	default:
		err := fmt.Errorf("redis: unknown message: %T", msg)
		log.Error().Err(err).Str("channel", channel).Msg("Redis Subscribe fail")
		return nil, err
	}
}

// 非协程安全，读取到信息才返回
func (s *Subscribe) Receive(ctx context.Context) (string, error) {
	if s.sub != nil {
		message, err := s.sub.ReceiveMessage(ctx) // Redis发生重连时 会返回错误，需要重新订阅
		if err == nil {
			return message.Payload, nil // 直接返回
		}

		s.sub.Close()
		s.sub = nil
	}

	// 重新创建，重新读取下
	var err error
	s.sub, err = s.r.subscribe(ctx, s.channel)
	if err != nil {
		return "", err
	}
	message, err := s.sub.ReceiveMessage(ctx)
	if err == nil {
		return message.Payload, nil
	}

	log.Error().Err(err).Msg("Redis Subscribe Receive fail")
	s.sub.Close()
	s.sub = nil
	return "", err
}
