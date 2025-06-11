import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MONGODB_URI = "mongodb+srv://Aragon07:Crystalys12@ipssi.xvoyfn7.mongodb.net/"
DB_NAME = "scraping"
COLLECTION_NAME = "articles"

BASE_URL = "https://www.blogdumoderateur.com"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def init_mongodb() -> MongoClient:
    """Initialise la connexion MongoDB"""
    client = MongoClient(MONGODB_URI)
    return client[DB_NAME][COLLECTION_NAME]

def clean_text(text: Optional[str]) -> Optional[str]:
    """Nettoie le texte en supprimant les espaces superflus"""
    return ' '.join(text.strip().split()) if text else None

def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Convertit une date française en format YYYY-MM-DD"""
    if not date_str:
        return None
    
    months_fr = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
    }
    
    try:
        parts = date_str.lower().split()
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month = months_fr.get(parts[1], '01')
            year = parts[2]
            return f"{year}-{month}-{day}"
    except Exception as e:
        logger.error(f"Erreur lors du parsing de la date {date_str}: {e}")
    return None

def fetch_article_content(url: str) -> Tuple[Optional[str], Dict[str, str], Optional[str], List[str]]:
    """Récupère le contenu détaillé d'un article"""
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        author = None
        author_tag = soup.select_one('.author a, .meta-author a, .byline a, .entry-meta a')
        if author_tag:
            author = clean_text(author_tag.get_text())
            logger.info(f"Auteur trouvé : {author}")
        else:
            logger.warning("Auteur non trouvé")
        
        subcategories = []
        subcategory_tags = soup.find_all('a', class_='post-tags')
        for tag in subcategory_tags:
            subcategory = clean_text(tag.get_text())
            if subcategory:
                subcategories.append(subcategory)
                logger.info(f"Sous-catégorie trouvée : {subcategory}")
        
        content_div = soup.find('div', class_='entry-content')
        if not content_div:
            return None, {}, author, subcategories
        
        content = ' '.join([
            p.get_text().strip() 
            for p in content_div.find_all(['p', 'h2', 'h3'])
        ])
        
        images = [
            img.get('src')
            for img in content_div.find_all('img')
            if img.get('src')
        ]
        
        return clean_text(content), images, author, subcategories
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du contenu de {url}: {e}")
        return None, {}, None, []  

def fetch_articles(url: str) -> List[Dict]:
    """Récupère les articles de la page principale"""
    collection = init_mongodb()
    articles_data = []
    
    try:
        logger.info(f"Tentative de récupération de la page : {url}")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        ignore_div = soup.select_one('div.container-fluid.px-md-8.pt-md-7.pt-5.pb-md-4.pb-1')
        if ignore_div:
            ignore_div.decompose() 
        
        articles = soup.find_all('article')
        logger.info(f"Nombre d'articles trouvés : {len(articles)}")
        
        for index, article in enumerate(articles, 1):
            try:
                logger.info(f"Traitement de l'article {index}/{len(articles)}")
                article_data = extract_article_data(article)
                
                if article_data:
                    content, images, author, subcategory = fetch_article_content(article_data['url'])
                    article_data.update({
                        'content': content,
                        'images': images,
                        'author': author,
                        'subcategory': subcategory
                    })
                    
                    collection.insert_one(article_data)
                    articles_data.append(article_data)
                    logger.info(f"Article ajouté : {article_data['title']}")
                else:
                    logger.warning(f"Article {index} ignoré : données non extraites")
                    
            except Exception as e:
                logger.error(f"Erreur lors du traitement de l'article {index} : {e}")
                continue
                
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des articles : {e}")
    
    return articles_data

def extract_article_data(article: BeautifulSoup) -> Optional[Dict]:
    """Extrait les données d'un article"""
    try:
        title_link = article.select_one('.post-title a, h2 a, article-title a, .entry-title a')
        
        if not title_link:
            possible_title = article.find(['h2', 'h3', 'h4'])
            if possible_title:
                logger.info(f"Utilisation du titre alternatif: {possible_title.get_text()}")
                title_link = possible_title.find('a') or possible_title.find_parent('a')
                if not title_link:
                    logger.warning("Pas de lien trouvé pour le titre alternatif")
                    return None
            else:
                logger.warning("Aucun titre trouvé")
                return None

        url = title_link.get('href', '')
        if not url.startswith('http'):
            url = url if url.startswith('/') else f"/{url}"
            url = f"{BASE_URL}{url}"
            
        logger.info(f"Titre trouvé : {title_link.get_text().strip()}")
        logger.info(f"URL trouvée : {url}")
            
        return {
            'title': clean_text(title_link.get_text()),
            'url': url,
            'thumbnail': extract_thumbnail(article),
            'category': extract_category(article),
            'resume': extract_excerpt(article),
            'publication_date': extract_date(article),
            'author': extract_author(article),
            'subcategory': extract_subcateg(article)  
        }
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des données : {e}")
        return None

def extract_thumbnail(article: BeautifulSoup) -> Optional[str]:
    """Extrait l'URL de la miniature"""
    img_tag = article.select_one('.entry-image img, .post-thumbnail img')
    if img_tag:
        return img_tag.get('src') 
    return None

def extract_subcateg(article: BeautifulSoup) -> Optional[str]:
    """Extrait la sous-catégorie"""
    subcategory_tag = article.select_one('a.post-tags') 
    return clean_text(subcategory_tag.get_text()) if subcategory_tag else None

def extract_category(article: BeautifulSoup) -> Optional[str]:
    """Extrait la catégorie"""
    category_tag = article.select_one('span.favtag.color-b') 
    return clean_text(category_tag.get_text()) if category_tag else None

def extract_excerpt(article: BeautifulSoup) -> Optional[str]:
    """Extrait le résumé"""
    excerpt_tag = article.select_one('div.entry-excerpt.t-def.t-size-def.pt-1')
    return clean_text(excerpt_tag.get_text()) if excerpt_tag else None

def extract_date(article: BeautifulSoup) -> Optional[str]:
    """Extrait la date"""
    date_tag = article.select_one('time.entry-date, .posted-on time')
    return parse_date(date_tag.get_text()) if date_tag else None

def extract_author(article: BeautifulSoup) -> Optional[str]:
    """Extrait l'auteur"""
    author_tag = article.select_one('.author a, .posted-by a')
    return clean_text(author_tag.get_text()) if author_tag else None

def find_articles_by_criteria(
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    author: Optional[str] = None,
    title_keywords: Optional[str] = None
) -> List[Dict]:
    """Recherche des articles selon différents critères"""
    collection = init_mongodb()
    query = {}
    
    if category:
        query['category'] = {'$regex': category, '$options': 'i'}
    
    if start_date or end_date:
        date_query = {}
        if start_date:
            date_query['$gte'] = start_date
        if end_date:
            date_query['$lte'] = end_date
        if date_query:
            query['publication_date'] = date_query
    
    if author:
        query['author'] = {'$regex': author, '$options': 'i'}
    
    if title_keywords:
        query['title'] = {'$regex': title_keywords, '$options': 'i'}
    
    return list(collection.find(query))

if __name__ == "__main__":
    try:
        logger.info("Début du scraping...")
        base_url = "https://www.blogdumoderateur.com/web"
        all_articles = []
        
        for page in range(1, 6):
            logger.info(f"\nTraitement de la page {page}")
            
            if page == 1:
                page_url = base_url + "/"
            else:
                page_url = f"{base_url}/page/{page}/"
                
            articles = fetch_articles(page_url)
            if articles:
                all_articles.extend(articles)
                logger.info(f"Articles trouvés sur la page {page}: {len(articles)}")
                logger.info(f"Total d'articles récupérés: {len(all_articles)}")
            else:
                logger.warning(f"Aucun article trouvé sur la page {page}")
                break  
            
        logger.info(f"\nNombre total d'articles récupérés : {len(all_articles)}")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution : {e}")

