package main

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

// secIDSet is the set of subscribed secids shared by the session goroutine,
// which adds the late catalog, and the catalog logger.
type secIDSet struct {
	mu  sync.Mutex
	ids map[int]struct{}
}

func newSecIDSet(initial map[int]struct{}) *secIDSet {
	set := &secIDSet{ids: make(map[int]struct{}, len(initial))}
	for id := range initial {
		set.ids[id] = struct{}{}
	}
	return set
}

func (s *secIDSet) add(ids map[int]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range ids {
		s.ids[id] = struct{}{}
	}
}

func (s *secIDSet) snapshot() map[int]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]struct{}, len(s.ids))
	for id := range s.ids {
		out[id] = struct{}{}
	}
	return out
}

// subscribeLateCatalog applies the export plan to securities that arrived
// after the session opened: they are written to transaq_securities,
// subscribed and, for bonds, asked for sec_info, like the catalog at open.
// The subscription follows the catalog, so a catalog sent in sections no
// longer freezes the feed on the first section (ofz-market/1.8).
func subscribeLateCatalog(ctx context.Context, session Session, cfg exportConfig, kinds []CandleKind,
	added []Security, subscribed *secIDSet) error {
	plan := buildExportPlan(Catalog{Securities: added, CandleKinds: kinds}, cfg)
	if err := insertSecurities(ctx, plan.Securities); err != nil {
		return fmt.Errorf("insert late securities: %w", err)
	}
	subscriber, canSubscribe := session.(Subscriber)
	if canSubscribe && (len(plan.AllTrades) > 0 || len(plan.Quotations) > 0) {
		if err := subscriber.Subscribe(ctx, plan.Subscription()); err != nil {
			return fmt.Errorf("subscribe late catalog: %w", err)
		}
		subscribed.add(plan.subscribedSecIDs())
	}
	secInfo := 0
	if source, ok := session.(SecuritiesInfoSource); ok && len(plan.SecInfo) > 0 {
		symbols := plan.secInfoSymbols()
		infos, failed, err := source.SecuritiesInfo(ctx, symbols)
		if err != nil {
			return fmt.Errorf("get late securities info: %w", err)
		}
		if err := insertSecInfoRows(ctx, infos); err != nil {
			return fmt.Errorf("insert late sec info: %w", err)
		}
		secInfo = len(infos)
		if failed > 0 {
			log.Warnf("late sec_info: %d of %d failed", failed, len(symbols))
		}
	}
	if len(plan.History) > 0 {
		log.Debugf("late catalog: %d history requests skipped", len(plan.History))
	}
	log.Infof("TRANSAQ catalog grew by %d securities: subscribe trades %d, quotations %d, sec_info %d",
		len(added), len(plan.AllTrades), len(plan.Quotations), secInfo)
	return nil
}
