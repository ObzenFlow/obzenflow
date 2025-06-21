//! Social Media Sentiment Analysis Pipeline
//! 
//! This example shows how to build a real-time sentiment analysis
//! pipeline using FlowState RS's beautiful DSL syntax with EventStore.
//! 
//! Run with: cargo run --example social_sentiment

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::VecDeque;

#[tokio::main]
async fn main() -> Result<()> {
    println!("💬 FlowState RS - Social Sentiment Analysis");
    println!("==========================================");
    
    // Create sample social media posts
    println!("📱 Creating sample social media posts...");
    let sample_posts = vec![
        ("Just had the most amazing experience with this new AI tool! Absolutely love it! 🎉", "twitter"),
        ("Customer service was terrible today. Worst experience ever. Very disappointed.", "facebook"),
        ("The weather is nice today. Going for a walk in the park.", "instagram"),
        ("This new feature is incredible! Game changer for productivity. Highly recommend!", "linkedin"),
        ("Ugh, this app keeps crashing. So frustrating and buggy. Needs major fixes.", "twitter"),
        ("Neutral update: scheduled maintenance tonight from 2-4 AM.", "company_blog"),
        ("Amazing breakthrough in renewable energy! This could change everything!", "reddit"),
        ("Meh, it's okay I guess. Nothing special but not terrible either.", "twitter"),
        ("Absolutely phenomenal customer support! They went above and beyond! 🌟", "facebook"),
        ("This is confusing and hard to understand. Poor documentation.", "github"),
    ];
    
    println!("✅ Created {} social media posts", sample_posts.len());
    println!("\n🔍 Running sentiment analysis pipeline...");
    
    // Create statistics trackers
    let positive_count = Arc::new(AtomicU64::new(0));
    let negative_count = Arc::new(AtomicU64::new(0));
    let neutral_count = Arc::new(AtomicU64::new(0));
    
    // Create temporary directory for event store
    // HERE'S THE BEAUTIFUL DSL SYNTAX WITH EVENTSTORE!
    let handle = flow! {
        name: "social_sentiment",
        flow_taxonomy: GoldenSignals,
        ("post_generator" => SocialPostGenerator::new(sample_posts), [RED::monitoring()])
        |> ("text_preprocessor" => TextPreprocessor::new(), [USE::monitoring()])
        |> ("sentiment_analyzer" => SentimentAnalyzer::new(), [GoldenSignals::monitoring()])
        |> ("trend_detector" => TrendDetector::new(), [SAAFE::monitoring()])
        |> ("sentiment_counter" => SentimentCounter::new(
            positive_count.clone(),
            negative_count.clone(),
            neutral_count.clone()
        ), [RED::monitoring()])
        |> ("results_logger" => ResultsLogger::new(), [USE::monitoring()])
    }?;
    
    // Let the flow process all events
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Gracefully shut down
    handle.shutdown().await?;
    
    println!("\n✅ Sentiment analysis completed!");
    
    // Show overall statistics
    let pos = positive_count.load(Ordering::Relaxed);
    let neg = negative_count.load(Ordering::Relaxed);
    let neu = neutral_count.load(Ordering::Relaxed);
    let total = pos + neg + neu;
    
    println!("\n📊 Overall Sentiment Statistics:");
    println!("  😊 Positive: {} ({:.1}%)", pos, (pos as f64 / total as f64) * 100.0);
    println!("  😔 Negative: {} ({:.1}%)", neg, (neg as f64 / total as f64) * 100.0);
    println!("  😐 Neutral:  {} ({:.1}%)", neu, (neu as f64 / total as f64) * 100.0);
    
    println!("\n💡 This pipeline demonstrates:");
    println!("   • Text preprocessing and cleaning");
    println!("   • Sentiment classification");
    println!("   • Real-time statistics aggregation");
    println!("   • Trend detection capabilities");
    println!("   • Beautiful, readable flow syntax");
    
    // Clean up
    // Cleanup handled by tempdir
    
    Ok(())
}

/// Generate social media posts as events
struct SocialPostGenerator {
    posts: VecDeque<(&'static str, &'static str)>,
    emitted: std::sync::atomic::AtomicBool,
}

impl SocialPostGenerator {
    fn new(posts: Vec<(&'static str, &'static str)>) -> Self {
        Self {
            posts: posts.into_iter().collect(),
            emitted: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl Step for SocialPostGenerator {
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        // Source steps generate events without input
        // Only emit posts once to avoid infinite generation
        if self.emitted.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return vec![];
        }
        
        let mut events = Vec::new();
        
        for (text, platform) in &self.posts {
            let event = ChainEvent::new("SocialPost", json!({
                "text": text,
                "platform": platform,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "user_id": format!("user_{}", fastrand::u32(1000..9999)),
            }));
            events.push(event);
        }
        
        println!("  📤 Generated {} social media posts", events.len());
        
        events
    }
}

/// Clean and preprocess text data
struct TextPreprocessor;

impl TextPreprocessor {
    fn new() -> Self {
        Self
    }
}

impl Step for TextPreprocessor {
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(text) = event.payload["text"].as_str() {
            // Clean the text
            let cleaned = text
                .to_lowercase()
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&nbsp;", " ");
            
            // Extract hashtags and mentions
            let hashtags: Vec<&str> = cleaned.matches("#").collect();
            let mentions: Vec<&str> = cleaned.matches("@").collect();
            
            // Calculate text metrics
            let word_count = cleaned.split_whitespace().count();
            let char_count = cleaned.len();
            
            event.payload["cleaned_text"] = json!(cleaned);
            event.payload["word_count"] = json!(word_count);
            event.payload["char_count"] = json!(char_count);
            event.payload["hashtag_count"] = json!(hashtags.len());
            event.payload["mention_count"] = json!(mentions.len());
            event.event_type = "PreprocessedPost".to_string();
        }
        
        vec![event]
    }
}

/// Analyze sentiment using keyword-based approach
struct SentimentAnalyzer;

impl SentimentAnalyzer {
    fn new() -> Self {
        Self
    }
}

impl Step for SentimentAnalyzer {
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(text) = event.payload["cleaned_text"].as_str() {
            let (sentiment, confidence) = analyze_sentiment(text);
            
            event.payload["sentiment"] = json!(sentiment);
            event.payload["confidence"] = json!(confidence);
            event.payload["original_text"] = event.payload["text"].clone();
            event.event_type = "AnalyzedPost".to_string();
        }
        
        vec![event]
    }
}

/// Detect trending topics and themes
struct TrendDetector;

impl TrendDetector {
    fn new() -> Self {
        Self
    }
}

impl Step for TrendDetector {
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(text) = event.payload["cleaned_text"].as_str() {
            let trending_topics = detect_trends(text);
            
            event.payload["trending_topics"] = json!(trending_topics);
            event.payload["is_trending"] = json!(!trending_topics.is_empty());
            event.event_type = "TrendAnalyzedPost".to_string();
        }
        
        vec![event]
    }
}

/// Count sentiment statistics
struct SentimentCounter {
    positive: Arc<AtomicU64>,
    negative: Arc<AtomicU64>,
    neutral: Arc<AtomicU64>,
}

impl SentimentCounter {
    fn new(positive: Arc<AtomicU64>, negative: Arc<AtomicU64>, neutral: Arc<AtomicU64>) -> Self {
        Self { 
            positive, 
            negative, 
            neutral,
        }
    }
}

impl Step for SentimentCounter {
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(sentiment) = event.payload["sentiment"].as_str() {
            match sentiment {
                "positive" => self.positive.fetch_add(1, Ordering::Relaxed),
                "negative" => self.negative.fetch_add(1, Ordering::Relaxed),
                _ => self.neutral.fetch_add(1, Ordering::Relaxed),
            };
        }
        
        vec![event]
    }
}

fn analyze_sentiment(text: &str) -> (&'static str, f64) {
    let positive_words = ["amazing", "love", "great", "awesome", "fantastic", "excellent", 
                         "wonderful", "incredible", "phenomenal", "brilliant", "good", "nice",
                         "recommend", "perfect", "beautiful", "outstanding"];
    let negative_words = ["terrible", "hate", "awful", "horrible", "worst", "bad", 
                         "disappointing", "disappointed", "frustrated", "frustrating", "annoying", 
                         "useless", "poor", "crashing", "buggy", "confusing", "hard", "meh",
                         "crashes", "sucks", "broken", "fail", "failed", "failing"];
    
    let text_lower = text.to_lowercase();
    
    let positive_count = positive_words.iter()
        .filter(|&&word| text_lower.contains(word))
        .count();
    
    let negative_count = negative_words.iter()
        .filter(|&&word| text_lower.contains(word))
        .count();
    
    let total_sentiment_words = positive_count + negative_count;
    
    if total_sentiment_words == 0 {
        ("neutral", 0.5)
    } else if positive_count > negative_count {
        let confidence = 0.6 + (positive_count as f64 / total_sentiment_words as f64) * 0.4;
        ("positive", confidence.min(1.0))
    } else if negative_count > positive_count {
        let confidence = 0.6 + (negative_count as f64 / total_sentiment_words as f64) * 0.4;
        ("negative", confidence.min(1.0))
    } else {
        ("neutral", 0.5)
    }
}

fn detect_trends(text: &str) -> Vec<String> {
    let trending_keywords = ["ai", "breakthrough", "innovation", "climate", "energy", 
                           "crypto", "blockchain", "startup", "ipo", "merger"];
    
    trending_keywords.iter()
        .filter(|&&keyword| text.to_lowercase().contains(keyword))
        .map(|&keyword| keyword.to_string())
        .collect()
}

/// Log results to console
struct ResultsLogger;

impl ResultsLogger {
    fn new() -> Self {
        Self
    }
}

impl Step for ResultsLogger {
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Log detailed results
        if let (Some(text), Some(sentiment), Some(confidence)) = (
            event.payload["original_text"].as_str(),
            event.payload["sentiment"].as_str(),
            event.payload["confidence"].as_f64(),
        ) {
            let emoji = match sentiment {
                "positive" => "😊",
                "negative" => "😔",
                _ => "😐",
            };
            
            let text_preview = if text.len() > 60 {
                format!("{}...", &text[..60])
            } else {
                text.to_string()
            };
            
            println!("  {} {} (confidence: {:.1}%): \"{}\"", 
                emoji, sentiment, confidence * 100.0, text_preview);
        }
        
        vec![] // Sink typically returns empty vec
    }
}