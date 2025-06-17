//! News to Script Pipeline Example
//! 
//! This demonstrates how clean and intuitive FlowState RS applications look
//! using the declarative DSL syntax for building data processing pipelines.
//! 
//! Run with: cargo run --example news_to_script

use flowstate_rs::prelude::*;
use flowstate_rs::{flow, event_sourcing::FlowHandle};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use chrono;

/// Source that generates news items
struct NewsSource {
    news_items: Vec<(String, String, String)>,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl NewsSource {
    fn new() -> Self {
        let news_items = vec![
            ("Breaking: Major AI Breakthrough Announced".to_string(), 
             "Scientists at leading tech companies announce significant progress in artificial general intelligence, with potential applications across multiple industries.".to_string(), 
             "technology".to_string()),
            ("Market Update: Tech Stocks Surge".to_string(), 
             "Technology stocks continue their upward trend as investors show confidence in AI and cloud computing sectors.".to_string(), 
             "finance".to_string()),
            ("Climate Tech Innovation: Solar Efficiency Soars".to_string(), 
             "New solar panel technology achieves record efficiency levels, promising more affordable renewable energy.".to_string(), 
             "environment".to_string()),
            ("Breaking: Quantum Computing Milestone".to_string(),
             "Researchers achieve quantum supremacy in practical applications, opening doors for drug discovery and cryptography breakthroughs.".to_string(),
             "technology".to_string()),
            ("Health Update: New Treatment Shows Promise".to_string(),
             "Clinical trials reveal groundbreaking results for novel cancer treatment approach using personalized medicine.".to_string(),
             "health".to_string()),
        ];
        
        Self {
            news_items,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("NewsSource"),
        }
    }
}

impl Step for NewsSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
        if current < self.news_items.len() {
            let (title, content, category) = &self.news_items[current];
            println!("📰 Processing: {}", title);
            vec![ChainEvent::new("NewsItem", json!({
                "title": title,
                "content": content,
                "category": category,
                "source": "sample_news",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }))]
        } else {
            vec![]
        }
    }
}

/// Extract and clean content from news items
struct ContentExtractor {
    metrics: <USE as Taxonomy>::Metrics,
}

impl ContentExtractor {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("ContentExtractor"),
        }
    }
}

impl Step for ContentExtractor {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "NewsItem" {
            // Extract and clean the content
            if let (Some(title), Some(content)) = (
                event.payload["title"].as_str(),
                event.payload["content"].as_str(),
            ) {
                // Simple content cleaning
                let cleaned_content = content
                    .replace("&nbsp;", " ")
                    .replace("&amp;", "&")
                    .trim()
                    .to_string();
                
                // Determine content type for script generation
                let content_type = if title.to_lowercase().contains("breaking") {
                    "breaking_news"
                } else if title.to_lowercase().contains("market") {
                    "market_analysis"
                } else if title.to_lowercase().contains("health") {
                    "health_update"
                } else {
                    "general_news"
                };
                
                event.payload["cleaned_content"] = json!(cleaned_content);
                event.payload["content_type"] = json!(content_type);
                event.payload["word_count"] = json!(cleaned_content.split_whitespace().count());
                event.event_type = "CleanedNews".to_string();
            }
        }
        vec![event]
    }
}

/// Generates YouTube scripts from news with quality scoring
struct ScriptGenerator {
    metrics: <GoldenSignals as Taxonomy>::Metrics,
}

impl ScriptGenerator {
    fn new() -> Self {
        Self {
            metrics: GoldenSignals::create_metrics("ScriptGenerator"),
        }
    }
    
    fn generate_script(&self, title: &str, content: &str, content_type: &str) -> (String, f64) {
        let intro = match content_type {
            "breaking_news" => "🚨 BREAKING NEWS ALERT! What's up everyone!",
            "market_analysis" => "📈 Welcome back investors and traders!",
            "health_update" => "💊 Hey health enthusiasts!",
            _ => "👋 What's up everyone!",
        };
        
        // Extract key points (simple implementation)
        let sentences: Vec<&str> = content.split(". ").collect();
        let key_points = sentences.iter()
            .take(3)
            .enumerate()
            .map(|(i, s)| format!("{}. {}", i + 1, s))
            .collect::<Vec<_>>()
            .join("\n   ");
        
        let script = format!(
            "🎬 {}\n\n\
            [INTRO - 0:00]\n\
            {} Today we're diving into something incredible: {}\n\n\
            [HOOK - 0:10]\n\
            But before we get started, if you're new here, hit that subscribe button and ring the notification bell so you never miss an update!\n\n\
            [MAIN CONTENT - 0:20]\n\
            Here's what you need to know:\n   {}\n\n\
            [DEEP DIVE - 1:00]\n\
            Let me break this down for you...\n\n\
            [ENGAGEMENT - 2:00]\n\
            What do you think about this? Drop your thoughts in the comments below!\n\n\
            [OUTRO - 2:30]\n\
            If you found this valuable, please give it a thumbs up and share it with someone who needs to see this. \
            Thanks for watching, and I'll catch you in the next one! Peace out! ✌️",
            title,
            intro,
            title.to_lowercase(),
            key_points
        );
        
        // Calculate quality score based on content
        let mut quality: f64 = 0.5; // Base score
        if content_type == "breaking_news" { quality += 0.2; }
        if content.len() > 100 { quality += 0.1; }
        if sentences.len() > 2 { quality += 0.1; }
        if title.contains("Breaking") || title.contains("Major") { quality += 0.1; }
        quality = quality.min(1.0);
        
        (script, quality)
    }
}

impl Step for ScriptGenerator {
    type Taxonomy = GoldenSignals;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &GoldenSignals
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "CleanedNews" {
            let title = event.payload.get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown Title");
            
            let content = event.payload.get("cleaned_content")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            
            let content_type = event.payload.get("content_type")
                .and_then(|v| v.as_str())
                .unwrap_or("general");
            
            let (script, quality) = self.generate_script(title, content, content_type);
            
            vec![ChainEvent::new("YouTubeScript", json!({
                "original_title": title,
                "category": event.payload.get("category").cloned().unwrap_or(json!("unknown")),
                "content_type": content_type,
                "script": script,
                "quality_score": quality,
                "generated_at": chrono::Utc::now().to_rfc3339(),
            }))]
        } else {
            vec![]
        }
    }
}

/// Filter scripts based on quality score
struct QualityFilter {
    min_score: f64,
    metrics: <USE as Taxonomy>::Metrics,
}

impl QualityFilter {
    fn min_score(min_score: f64) -> Self {
        Self { 
            min_score,
            metrics: USE::create_metrics("QualityFilter"),
        }
    }
}

impl Step for QualityFilter {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(quality_score) = event.payload["quality_score"].as_f64() {
            if quality_score >= self.min_score {
                println!("   ✅ High-quality script accepted (score: {:.2})", quality_score);
                vec![event]
            } else {
                println!("   ❌ Low-quality script filtered (score: {:.2})", quality_score);
                vec![] // Filter out low-quality scripts
            }
        } else {
            vec![event] // Pass through if no quality score
        }
    }
}

/// Sink that collects and displays scripts
struct ScriptCollectorSink {
    scripts: Arc<tokio::sync::Mutex<Vec<ChainEvent>>>,
    metrics: <SAAFE as Taxonomy>::Metrics,
}

impl ScriptCollectorSink {
    fn new() -> (Self, Arc<tokio::sync::Mutex<Vec<ChainEvent>>>) {
        let scripts = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        (Self {
            scripts: scripts.clone(),
            metrics: SAAFE::create_metrics("ScriptCollectorSink"),
        }, scripts)
    }
}

impl Step for ScriptCollectorSink {
    type Taxonomy = SAAFE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &SAAFE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "YouTubeScript" {
            let scripts = self.scripts.clone();
            tokio::spawn(async move {
                scripts.lock().await.push(event);
            });
        }
        vec![] // Sinks don't emit
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 FlowState RS - News to YouTube Scripts");
    println!("========================================");
    println!("📰 Creating sample news data...");
    
    let (sink, scripts) = ScriptCollectorSink::new();
    
    println!("\n🎬 Running news-to-script pipeline...");
    
    // HERE'S THE BEAUTIFUL DSL SYNTAX! 🎉
    let handle: FlowHandle = flow! {
        name: "news_to_script",
        flow_taxonomy: GoldenSignals,
        ("news" => NewsSource::new(), RED)
        |> ("extractor" => ContentExtractor::new(), USE) 
        |> ("generator" => ScriptGenerator::new(), GoldenSignals)
        |> ("filter" => QualityFilter::min_score(0.6), USE)
        |> ("output" => sink, SAAFE)
    }?;
    
    // Allow time for pipeline to initialize
    println!("⏳ Initializing pipeline...");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Let it run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;
    
    println!("\n✅ Pipeline completed!");
    
    // Show the results
    let final_scripts = scripts.lock().await;
    println!("\n📺 Generated YouTube Scripts:");
    println!("═══════════════════════════════");
    
    for (i, script) in final_scripts.iter().enumerate() {
        if let Some(title) = script.payload["original_title"].as_str() {
            println!("\n{}. 🎬 {}", i + 1, title);
            
            if let Some(content_type) = script.payload["content_type"].as_str() {
                println!("   📌 Type: {}", content_type);
            }
            
            if let Some(script_text) = script.payload["script"].as_str() {
                let preview = if script_text.len() > 100 {
                    format!("{}...", &script_text[..100])
                } else {
                    script_text.to_string()
                };
                println!("   📝 Preview: {}", preview);
            }
            
            if let Some(quality) = script.payload["quality_score"].as_f64() {
                let stars = "⭐".repeat((quality * 5.0) as usize);
                println!("   {} Quality: {:.1}/10", stars, quality * 10.0);
            }
        }
    }
    
    println!("\n💡 This is how FlowState RS applications should look:");
    println!("   • Clean, declarative flow syntax");
    println!("   • Business logic in reusable stages"); 
    println!("   • Self-documenting pipeline structure");
    println!("   • Quality filtering and metrics built-in");
    println!("   • Easy to read, modify, and extend");
    
    println!("\n🎯 Key Features Demonstrated:");
    println!("   • Content extraction and cleaning");
    println!("   • Dynamic script generation based on content type");
    println!("   • Quality scoring and filtering");
    println!("   • Metrics collection with different taxonomies");
    
    // Cleanup
    // Cleanup handled by tempdir
    Ok(())
}